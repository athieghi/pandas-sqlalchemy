dataset.to_sql(
    "Tickers", 
    con = engine,
    if_exists = "replace",
    schema='dbo',   
    index=False,
    chunksize=1000
#    dtype=tickerDeparaDb
)