#!/usr/bin/env python3
"""
App 缓存读取适配器（TradingAgents -> app MongoDB 集合）
- 基本信息集合：stock_basic_info
- 行情集合：market_quotes

当启用 ta_use_app_cache 时，作为优先数据源；未命中部分由上层继续回退到直连数据源。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, time as dtime

import pandas as pd
import logging

_logger = logging.getLogger('dataflows')

try:
    from tradingagents.config.database_manager import get_mongodb_client
except Exception:  # pragma: no cover - 弱依赖
    get_mongodb_client = None  # type: ignore


BASICS_COLLECTION = "stock_basic_info"
QUOTES_COLLECTION = "market_quotes"

# 行情数据最大允许过期时间（秒），交易时段内超过此阈值将被视为过期
_DEFAULT_MAX_STALENESS_SECONDS = 300


def _is_trading_time(now: Optional[datetime] = None) -> bool:
    """判断当前是否在 A 股交易时段内（含收盘后缓冲 30 分钟）"""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Shanghai")
    except Exception:
        return True  # 无法判断时区，保守视为交易中
    now = now or datetime.now(tz)
    if now.weekday() > 4:
        return False
    t = now.time()
    return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 30))


def get_basics_from_cache(stock_code: Optional[str] = None) -> Optional[Dict[str, Any] | List[Dict[str, Any]]]:
    """从 app 的 stock_basic_info 读取基础信息。"""
    if get_mongodb_client is None:
        return None
    client = get_mongodb_client()
    if not client:
        return None
    try:
        # 数据库名取自 DatabaseManager 内部配置
        db_name = None
        try:
            # 访问 DatabaseManager 暴露的配置
            from tradingagents.config.database_manager import get_database_manager  # type: ignore
            db_name = get_database_manager().mongodb_config.get("database", "tradingagents")
        except Exception:
            db_name = "tradingagents"
        db = client[db_name]
        coll = db[BASICS_COLLECTION]
        if stock_code:
            code6 = str(stock_code).zfill(6)
            try:
                _logger.debug(f"[app_cache] 查询基础信息 | db={db_name} coll={BASICS_COLLECTION} code={code6}")
            except Exception:
                pass
            # 同时查询 symbol 和 code 字段，确保兼容新旧数据格式
            doc = coll.find_one({"$or": [{"symbol": code6}, {"code": code6}]})
            if not doc:
                try:
                    _logger.debug(f"[app_cache] 基础信息未命中 | db={db_name} coll={BASICS_COLLECTION} code={code6}")
                except Exception:
                    pass
            return doc or None
        else:
            cursor = coll.find({})
            docs = list(cursor)
            return docs or None
    except Exception as e:
        try:
            _logger.debug(f"[app_cache] 基础信息读取异常（忽略）: {e}")
        except Exception:
            pass
        return None


def get_market_quote_dataframe(symbol: str, max_staleness_seconds: int = _DEFAULT_MAX_STALENESS_SECONDS) -> Optional[pd.DataFrame]:
    """从 app 的 market_quotes 读取单只股票的最新一条快照，并转为 DataFrame。

    Args:
        symbol: 6 位股票代码
        max_staleness_seconds: 交易时段内最大允许过期秒数，超时返回 None 以触发实时回退。
                               设为 0 禁用过期检查。默认 300（5 分钟）。
    """
    if get_mongodb_client is None:
        return None
    client = get_mongodb_client()
    if not client:
        return None
    try:
        from tradingagents.config.database_manager import get_database_manager  # type: ignore
        db_name = get_database_manager().mongodb_config.get("database", "tradingagents")
        db = client[db_name]
        coll = db[QUOTES_COLLECTION]
        code = str(symbol).zfill(6)
        try:
            _logger.debug(f"[app_cache] 查询行情 | db={db_name} coll={QUOTES_COLLECTION} code={code}")
        except Exception:
            pass
        doc = coll.find_one({"code": code})
        if not doc:
            try:
                _logger.debug(f"[app_cache] 行情未命中 | db={db_name} coll={QUOTES_COLLECTION} code={code}")
            except Exception:
                pass
            return None

        # 新鲜度检查：交易时段内过期数据返回 None，触发实时回退
        if max_staleness_seconds > 0 and _is_trading_time():
            updated_at = doc.get("updated_at")
            if updated_at is not None:
                try:
                    if isinstance(updated_at, datetime):
                        age = (datetime.now(updated_at.tzinfo) - updated_at).total_seconds()
                        if age > max_staleness_seconds:
                            _logger.info(
                                f"[app_cache] 行情过期 | code={code} age={age:.0f}s > {max_staleness_seconds}s，"
                                f"触发实时回退"
                            )
                            return None
                except Exception:
                    pass  # 时间比较失败不阻塞

        row = {
            "code": code,
            "date": doc.get("trade_date"),
            "open": doc.get("open"),
            "high": doc.get("high"),
            "low": doc.get("low"),
            "close": doc.get("close"),
            "volume": doc.get("volume"),
            "amount": doc.get("amount"),
            "pct_chg": doc.get("pct_chg"),
            "change": None,
        }
        df = pd.DataFrame([row])
        return df
    except Exception as e:
        try:
            _logger.debug(f"[app_cache] 行情读取异常（忽略）: {e}")
        except Exception:
            pass
        return None

