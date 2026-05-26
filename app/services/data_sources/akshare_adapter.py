"""
AKShare data source adapter
"""
from typing import Optional, Dict
import logging
import time
from datetime import datetime, timedelta
import pandas as pd

from .base import DataSourceAdapter

logger = logging.getLogger(__name__)

# 模拟浏览器请求头，避免被数据源反爬
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# 保存原始 requests.get，用于注入请求头
try:
    import requests as _requests_mod
    _original_requests_get = _requests_mod.get
except Exception:
    _original_requests_get = None


def _patched_requests_get(url, **kwargs):
    """注入浏览器请求头的 requests.get 包装"""
    kwargs.setdefault("headers", {})
    for k, v in _BROWSER_HEADERS.items():
        kwargs["headers"].setdefault(k, v)
    kwargs.setdefault("timeout", 30)
    return _original_requests_get(url, **kwargs)


def _retry_with_backoff(func, max_retries=3, base_delay=2.0):
    """带指数退避的重试装饰器"""
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"第 {attempt + 1}/{max_retries} 次重试，等待 {delay:.0f}s: {e}")
                time.sleep(delay)
            else:
                raise last_exc


class AKShareAdapter(DataSourceAdapter):
    """AKShare数据源适配器"""

    def __init__(self):
        super().__init__()  # 调用父类初始化

    @property
    def name(self) -> str:
        return "akshare"

    def _get_default_priority(self) -> int:
        return 2  # 数字越大优先级越高

    def is_available(self) -> bool:
        """检查AKShare是否可用"""
        try:
            import akshare as ak  # noqa: F401
            return True
        except ImportError:
            return False

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表（使用 AKShare 的 stock_info_a_code_name 接口获取真实股票名称）"""
        if not self.is_available():
            return None
        try:
            import akshare as ak
            logger.info("AKShare: Fetching stock list with real names from stock_info_a_code_name()...")

            # 使用 AKShare 的 stock_info_a_code_name 接口获取股票代码和名称
            df = ak.stock_info_a_code_name()

            if df is None or df.empty:
                logger.warning("AKShare: stock_info_a_code_name() returned empty data")
                return None

            # 标准化列名（AKShare 返回的列名可能是中文）
            # 通常返回的列：code（代码）、name（名称）
            df = df.rename(columns={
                'code': 'symbol',
                '代码': 'symbol',
                'name': 'name',
                '名称': 'name'
            })

            # 确保有必需的列
            if 'symbol' not in df.columns or 'name' not in df.columns:
                logger.error(f"AKShare: Unexpected column names: {df.columns.tolist()}")
                return None

            # 生成 ts_code 和其他字段
            def generate_ts_code(code: str) -> str:
                """根据股票代码生成 ts_code"""
                if not code:
                    return ""
                code = str(code).zfill(6)
                if code.startswith(('60', '68', '90')):
                    return f"{code}.SH"
                elif code.startswith(('00', '30', '20')):
                    return f"{code}.SZ"
                elif code.startswith(('8', '4')):
                    return f"{code}.BJ"
                else:
                    return f"{code}.SZ"  # 默认深圳

            def get_market(code: str) -> str:
                """根据股票代码判断市场"""
                if not code:
                    return ""
                code = str(code).zfill(6)
                if code.startswith('000'):
                    return '主板'
                elif code.startswith('002'):
                    return '中小板'
                elif code.startswith('300'):
                    return '创业板'
                elif code.startswith('60'):
                    return '主板'
                elif code.startswith('688'):
                    return '科创板'
                elif code.startswith('8'):
                    return '北交所'
                elif code.startswith('4'):
                    return '新三板'
                else:
                    return '未知'

            # 添加 ts_code 和 market 字段
            df['ts_code'] = df['symbol'].apply(generate_ts_code)
            df['market'] = df['symbol'].apply(get_market)
            df['area'] = ''
            df['industry'] = ''
            df['list_date'] = ''

            logger.info(f"AKShare: Successfully fetched {len(df)} stocks with real names")
            return df

        except Exception as e:
            logger.error(f"AKShare: Failed to fetch stock list: {e}")
            return None

    def get_daily_basic(self, trade_date: str) -> Optional[pd.DataFrame]:
        """获取每日基础财务数据（快速版）"""
        if not self.is_available():
            return None
        try:
            import akshare as ak  # noqa: F401
            logger.info(f"AKShare: Attempting to get basic financial data for {trade_date}")

            stock_df = self.get_stock_list()
            if stock_df is None or stock_df.empty:
                logger.warning("AKShare: No stock list available")
                return None

            max_stocks = 10
            stock_list = stock_df.head(max_stocks)

            basic_data = []
            processed_count = 0
            import time
            start_time = time.time()
            timeout_seconds = 30

            for _, stock in stock_list.iterrows():
                if time.time() - start_time > timeout_seconds:
                    logger.warning(f"AKShare: Timeout reached, processed {processed_count} stocks")
                    break
                try:
                    symbol = stock.get('symbol', '')
                    name = stock.get('name', '')
                    ts_code = stock.get('ts_code', '')
                    if not symbol:
                        continue
                    info_data = ak.stock_individual_info_em(symbol=symbol)
                    if info_data is not None and not info_data.empty:
                        info_dict = {}
                        for _, row in info_data.iterrows():
                            item = row.get('item', '')
                            value = row.get('value', '')
                            info_dict[item] = value
                        latest_price = self._safe_float(info_dict.get('最新', 0))
                        # 🔥 AKShare 的"总市值"单位是万元，需要转换为亿元（与 Tushare 一致）
                        total_mv_wan = self._safe_float(info_dict.get('总市值', 0))  # 万元
                        total_mv_yi = total_mv_wan / 10000 if total_mv_wan else None  # 转换为亿元
                        basic_data.append({
                            'ts_code': ts_code,
                            'trade_date': trade_date,
                            'name': name,
                            'close': latest_price,
                            'total_mv': total_mv_yi,  # 亿元（与 Tushare 一致）
                            'turnover_rate': None,
                            'pe': None,
                            'pb': None,
                        })
                        processed_count += 1
                        if processed_count % 5 == 0:
                            logger.debug(f"AKShare: Processed {processed_count} stocks in {time.time() - start_time:.1f}s")
                except Exception as e:
                    logger.debug(f"AKShare: Failed to get data for {symbol}: {e}")
                    continue

            if basic_data:
                df = pd.DataFrame(basic_data)
                logger.info(f"AKShare: Successfully fetched basic data for {trade_date}, {len(df)} records")
                return df
            else:
                logger.warning("AKShare: No basic data collected")
                return None
        except Exception as e:
            logger.error(f"AKShare: Failed to fetch basic data for {trade_date}: {e}")
            return None

    def _do_fetch_quotes(self, source: str) -> Optional[Dict[str, Dict[str, Optional[float]]]]:
        """实际执行行情获取（内部方法，由 get_realtime_quotes 调用）"""
        import akshare as ak  # type: ignore

        if source == "sina":
            df = ak.stock_zh_a_spot()
            logger.info("使用 AKShare 新浪财经接口获取实时行情")
        else:
            df = ak.stock_zh_a_spot_em()
            logger.info("使用 AKShare 东方财富接口获取实时行情")

        if df is None or getattr(df, "empty", True):
            logger.warning(f"AKShare {source} 返回空数据")
            return None

        code_col = next((c for c in ["代码", "code", "symbol", "股票代码"] if c in df.columns), None)
        price_col = next((c for c in ["最新价", "现价", "最新价(元)", "price", "最新", "trade"] if c in df.columns), None)
        pct_col = next((c for c in ["涨跌幅", "涨跌幅(%)", "涨幅", "pct_chg", "changepercent"] if c in df.columns), None)
        amount_col = next((c for c in ["成交额", "成交额(元)", "amount", "成交额(万元)", "amount(万元)"] if c in df.columns), None)
        open_col = next((c for c in ["今开", "开盘", "open", "今开(元)"] if c in df.columns), None)
        high_col = next((c for c in ["最高", "high"] if c in df.columns), None)
        low_col = next((c for c in ["最低", "low"] if c in df.columns), None)
        pre_close_col = next((c for c in ["昨收", "昨收(元)", "pre_close", "昨收价", "settlement"] if c in df.columns), None)
        volume_col = next((c for c in ["成交量", "成交量(手)", "volume", "成交量(股)", "vol"] if c in df.columns), None)

        if not code_col or not price_col:
            logger.error(f"AKShare {source} 缺少必要列: code={code_col}, price={price_col}, columns={list(df.columns)}")
            return None

        result: Dict[str, Dict[str, Optional[float]]] = {}
        for _, row in df.iterrows():
            code_raw = row.get(code_col)
            if not code_raw:
                continue
            code_str = str(code_raw).strip()
            if len(code_str) > 6:
                code_str = ''.join(filter(str.isdigit, code_str))
            if code_str.isdigit():
                code = code_str.lstrip('0').zfill(6) or '000000'
            else:
                code_digits = ''.join(filter(str.isdigit, code_str))
                if code_digits:
                    code = code_digits.zfill(6)
                else:
                    continue

            result[code] = {
                "close": self._safe_float(row.get(price_col)),
                "pct_chg": self._safe_float(row.get(pct_col)) if pct_col else None,
                "amount": self._safe_float(row.get(amount_col)) if amount_col else None,
                "volume": self._safe_float(row.get(volume_col)) if volume_col else None,
                "open": self._safe_float(row.get(open_col)) if open_col else None,
                "high": self._safe_float(row.get(high_col)) if high_col else None,
                "low": self._safe_float(row.get(low_col)) if low_col else None,
                "pre_close": self._safe_float(row.get(pre_close_col)) if pre_close_col else None,
            }
        return result

    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        try:
            if value is None or value == '' or value == 'None':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_realtime_quotes(self, source: str = "eastmoney"):
        """
        获取全市场实时快照，返回以6位代码为键的字典

        优化策略：
        1. 注入浏览器请求头避免反爬
        2. 失败自动重试（3次，指数退避）
        3. 东方财富失败后自动降级到新浪财经

        Args:
            source: 数据源选择，"eastmoney"（东方财富）或 "sina"（新浪财经）

        Returns:
            Dict[str, Dict]: {code: {close, pct_chg, amount, ...}}
        """
        if not self.is_available():
            return None

        # 注入浏览器请求头
        import requests as _requests
        _requests.get = _patched_requests_get

        try:
            sources_to_try = [source] if source == "sina" else [source, "sina"]

            for src in sources_to_try:
                try:
                    result = _retry_with_backoff(
                        lambda s=src: self._do_fetch_quotes(s),
                        max_retries=3,
                        base_delay=2.0
                    )
                    if result:
                        logger.info(f"✅ AKShare {src} 获取到 {len(result)} 只股票的实时行情")
                        if src != source:
                            logger.info(f"   ℹ️ 已从 {source} 自动降级到 {src}")
                        return result
                except Exception as e:
                    logger.warning(f"AKShare {src} 获取失败（已重试）: {e}")
                    continue

            logger.error(f"AKShare 所有数据源均失败: {sources_to_try}")
            return None

        finally:
            # 恢复原始 requests.get
            _requests.get = _original_requests_get

    def get_kline(self, code: str, period: str = "day", limit: int = 120, adj: Optional[str] = None):
        """AKShare K-line as fallback. Try daily/week/month via stock_zh_a_hist; minutes via stock_zh_a_minute."""
        if not self.is_available():
            return None
        try:
            import akshare as ak
            code6 = str(code).zfill(6)
            items = []
            if period in ("day", "week", "month"):
                period_map = {"day": "daily", "week": "weekly", "month": "monthly"}
                adjust_map = {None: "", "qfq": "qfq", "hfq": "hfq"}
                df = ak.stock_zh_a_hist(symbol=code6, period=period_map[period], adjust=adjust_map.get(adj, ""))
                if df is None or getattr(df, 'empty', True):
                    return None
                df = df.tail(limit)
                for _, row in df.iterrows():
                    items.append({
                        "time": str(row.get('日期') or row.get('date') or ''),
                        "open": self._safe_float(row.get('开盘') or row.get('open')),
                        "high": self._safe_float(row.get('最高') or row.get('high')),
                        "low": self._safe_float(row.get('最低') or row.get('low')),
                        "close": self._safe_float(row.get('收盘') or row.get('close')),
                        "volume": self._safe_float(row.get('成交量') or row.get('volume')),
                        "amount": self._safe_float(row.get('成交额') or row.get('amount')),
                    })
                return items
            else:
                # minutes
                per_map = {"5m": "5", "15m": "15", "30m": "30", "60m": "60"}
                if period not in per_map:
                    return None
                df = ak.stock_zh_a_minute(symbol=code6, period=per_map[period], adjust=adj if adj in ("qfq", "hfq") else "")
                if df is None or getattr(df, 'empty', True):
                    return None
                df = df.tail(limit)
                for _, row in df.iterrows():
                    items.append({
                        "time": str(row.get('时间') or row.get('day') or ''),
                        "open": self._safe_float(row.get('开盘') or row.get('open')),
                        "high": self._safe_float(row.get('最高') or row.get('high')),
                        "low": self._safe_float(row.get('最低') or row.get('low')),
                        "close": self._safe_float(row.get('收盘') or row.get('close')),
                        "volume": self._safe_float(row.get('成交量') or row.get('volume')),
                        "amount": self._safe_float(row.get('成交额') or row.get('amount')),
                    })
                return items
        except Exception as e:
            logger.error(f"AKShare get_kline failed: {e}")
            return None

    def get_news(self, code: str, days: int = 2, limit: int = 50, include_announcements: bool = True):
        """AKShare-based news/announcements fallback"""
        if not self.is_available():
            return None
        try:
            import akshare as ak
            code6 = str(code).zfill(6)
            items = []
            # news
            try:
                dfn = ak.stock_news_em(symbol=code6)
                if dfn is not None and not dfn.empty:
                    for _, row in dfn.head(limit).iterrows():
                        items.append({
                            # AkShare 将字段标准化为中文列名：新闻标题 / 文章来源 / 发布时间 / 新闻链接
                            "title": str(row.get('新闻标题') or row.get('标题') or row.get('title') or ''),
                            "source": str(row.get('文章来源') or row.get('来源') or row.get('source') or 'akshare'),
                            "time": str(row.get('发布时间') or row.get('time') or ''),
                            "url": str(row.get('新闻链接') or row.get('url') or ''),
                            "type": "news",
                        })
            except Exception:
                pass
            # announcements
            try:
                if include_announcements:
                    dfa = ak.stock_announcement_em(symbol=code6)
                    if dfa is not None and not dfa.empty:
                        for _, row in dfa.head(max(0, limit - len(items))).iterrows():
                            items.append({
                                "title": str(row.get('公告标题') or row.get('title') or ''),
                                "source": "akshare",
                                "time": str(row.get('公告时间') or row.get('time') or ''),
                                "url": str(row.get('公告链接') or row.get('url') or ''),
                                "type": "announcement",
                            })
            except Exception:
                pass
            return items if items else None
        except Exception as e:
            logger.error(f"AKShare get_news failed: {e}")
            return None

    def find_latest_trade_date(self) -> Optional[str]:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        logger.info(f"AKShare: Using yesterday as trade date: {yesterday}")
        return yesterday

