print("stock_daily_quotes count: " + db.getSiblingDB("tradingagentscn").stock_daily_quotes.countDocuments({}));
print("Unique symbols: " + db.getSiblingDB("tradingagentscn").stock_daily_quotes.distinct("symbol").length);
print("Has 600498: " + db.getSiblingDB("tradingagentscn").stock_daily_quotes.countDocuments({symbol: "600498"}));
