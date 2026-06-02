from bot.config import load_config
from bot.storage import Store

cfg = load_config()
store = Store(cfg.db_path)
for table in ("option_chain_snapshots", "quote_snapshots"):
    try:
        df = store.query_df(f"SELECT coalesce(source, 'UNTAGGED') AS source, count(*) AS rows FROM {table} GROUP BY 1 ORDER BY rows DESC")
        print(f"\n{table}")
        print(df.to_string(index=False))
    except Exception as exc:
        print(f"{table}: {exc}")
