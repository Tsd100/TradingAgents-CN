#!/usr/bin/env python3
"""
BaoStock daily K-line sync script — populates stock_daily_quotes from BaoStock.
"""
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import asyncio
import pandas as pd
from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
MONGO_URI = "mongodb://admin:tradingagents123@localhost:27017/tradingagentscn?authSource=admin"
DB_NAME = "tradingagentscn"
LOOKBACK_DAYS = 365
BATCH_SIZE = 10
DELAY_BETWEEN_STOCKS = 0.3  # seconds between individual stocks
DELAY_BETWEEN_BATCHES = 2.0  # seconds between batches


def get_baostock_code(code: str) -> str:
    """Convert 6-digit code to BaoStock format."""
    if code.startswith(('60', '68', '90')):
        return f"sh.{code}"
    else:
        return f"sz.{code}"


async def fetch_baostock_data(code: str, start_date: str, end_date: str):
    """Fetch daily K-line data from BaoStock for a single stock."""
    import baostock as bs_lib

    bs_code = get_baostock_code(code)

    def _fetch():
        lg = bs_lib.login()
        if lg.error_code != '0':
            raise Exception(f"Login failed: {lg.error_msg}")
        try:
            fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
            rs = bs_lib.query_history_k_data_plus(
                code=bs_code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )
            if rs.error_code != '0':
                raise Exception(f"Query failed: {rs.error_msg}")
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            return data_list, rs.fields
        finally:
            bs_lib.logout()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch)


async def sync_all_stocks(limit: int = None, mongo_uri: str = MONGO_URI):
    """Sync daily K-line data for all stocks from BaoStock."""
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]
    collection = db.stock_daily_quotes

    # Get all stock codes
    stock_codes = list(db.stock_basic_info.distinct("code", {"source": "baostock"}))
    if limit:
        stock_codes = stock_codes[:limit]

    total = len(stock_codes)
    logger.info(f"Starting sync for {total} stocks ({LOOKBACK_DAYS} days lookback)")

    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Date range: {start_date} to {end_date}")

    total_inserted = 0
    total_updated = 0
    errors = 0
    now = datetime.utcnow()

    for batch_start in range(0, total, BATCH_SIZE):
        batch = stock_codes[batch_start:batch_start + BATCH_SIZE]
        ops = []

        for code in batch:
            try:
                data_list, fields = await fetch_baostock_data(code, start_date, end_date)

                if not data_list:
                    logger.debug(f"{code}: no data returned")
                    continue

                for row in data_list:
                    record = dict(zip(fields, row))
                    trade_date = record.get("date", "").strip()
                    if not trade_date:
                        continue

                    # Skip non-trading days
                    if record.get("tradestatus", "1") != "1":
                        continue

                    full_symbol = code
                    if code.startswith(('60', '68', '90')):
                        full_symbol = f"{code}.SH"
                    else:
                        full_symbol = f"{code}.SZ"

                    def safe_float(v):
                        try:
                            if v is None or v == '' or v == 'None':
                                return None
                            return float(v)
                        except (ValueError, TypeError):
                            return None

                    doc = {
                        "symbol": code,
                        "code": code,
                        "full_symbol": full_symbol,
                        "market": "CN",
                        "trade_date": trade_date,
                        "period": "daily",
                        "data_source": "baostock",
                        "open": safe_float(record.get("open")),
                        "high": safe_float(record.get("high")),
                        "low": safe_float(record.get("low")),
                        "close": safe_float(record.get("close")),
                        "pre_close": safe_float(record.get("preclose")),
                        "volume": safe_float(record.get("volume")),
                        "amount": safe_float(record.get("amount")),
                        "change": safe_float(record.get("pctChg")),
                        "pct_chg": safe_float(record.get("pctChg")),
                        "turnover": safe_float(record.get("turn")),
                        "created_at": now,
                        "updated_at": now,
                        "version": 1,
                    }

                    # Upsert: unique on (symbol, trade_date, period, data_source)
                    ops.append(UpdateOne(
                        {
                            "symbol": code,
                            "trade_date": trade_date,
                            "period": "daily",
                            "data_source": "baostock"
                        },
                        {"$set": doc},
                        upsert=True
                    ))

                # Delay between stocks to avoid rate limiting
                await asyncio.sleep(DELAY_BETWEEN_STOCKS)

            except Exception as e:
                logger.error(f"Error processing {code}: {e}")
                errors += 1

        # Execute batch
        if ops:
            try:
                result = collection.bulk_write(ops, ordered=False)
                total_inserted += result.upserted_count
                total_updated += result.modified_count
            except Exception as e:
                logger.error(f"Bulk write failed for batch: {e}")
                errors += len(batch)

        progress_pct = min(batch_start + BATCH_SIZE, total) / total * 100
        logger.info(
            f"Progress: {min(batch_start + BATCH_SIZE, total)}/{total} ({progress_pct:.1f}%) | "
            f"Inserted: {total_inserted}, Updated: {total_updated}, Errors: {errors}"
        )

        if batch_start + BATCH_SIZE < total:
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)

    client.close()
    logger.info(f"Sync complete! Total inserted: {total_inserted}, updated: {total_updated}, errors: {errors}")
    return total_inserted, total_updated, errors


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync daily K-line data from BaoStock")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of stocks (for testing)")
    parser.add_argument("--mongo-uri", type=str, default=None,
                        help="MongoDB URI override")
    args = parser.parse_args()

    mongo_uri = args.mongo_uri or MONGO_URI
    asyncio.run(sync_all_stocks(limit=args.limit, mongo_uri=mongo_uri))
