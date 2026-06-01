from __future__ import annotations

from pathlib import Path
import sqlite3
import pandas as pd


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.backend = "duckdb"
        try:
            import duckdb  # type: ignore
            self.con = duckdb.connect(str(self.db_path))
        except Exception:
            self.backend = "sqlite"
            if self.db_path.suffix == ".duckdb":
                self.db_path = self.db_path.with_suffix(".sqlite")
            self.con = sqlite3.connect(str(self.db_path))
        self.init_schema()

    def execute(self, sql: str, params: tuple | list | None = None):
        if params is None:
            return self.con.execute(sql)
        return self.con.execute(sql, params)

    def init_schema(self) -> None:
        self.execute("""
        CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            ts TIMESTAMP,
            trade_date DATE,
            expiry DATE,
            strike DOUBLE,
            option_type VARCHAR,
            trading_symbol VARCHAR,
            underlying_ltp DOUBLE,
            ltp DOUBLE,
            open_interest DOUBLE,
            volume DOUBLE,
            delta DOUBLE,
            gamma DOUBLE,
            theta DOUBLE,
            vega DOUBLE,
            rho DOUBLE,
            iv DOUBLE,
            raw_json VARCHAR,
            source VARCHAR
        );
        """)
        self.execute("""
        CREATE TABLE IF NOT EXISTS quote_snapshots (
            ts TIMESTAMP,
            trade_date DATE,
            trading_symbol VARCHAR,
            last_price DOUBLE,
            bid_price DOUBLE,
            bid_quantity DOUBLE,
            offer_price DOUBLE,
            offer_quantity DOUBLE,
            spread DOUBLE,
            spread_pct DOUBLE,
            open_interest DOUBLE,
            volume DOUBLE,
            implied_volatility DOUBLE,
            total_buy_quantity DOUBLE,
            total_sell_quantity DOUBLE,
            last_trade_quantity DOUBLE,
            last_trade_time BIGINT,
            depth_json VARCHAR,
            raw_json VARCHAR,
            source VARCHAR
        );
        """)
        self.execute("""
        CREATE TABLE IF NOT EXISTS model_signals (
            ts TIMESTAMP,
            trading_symbol VARCHAR,
            expiry DATE,
            strike DOUBLE,
            option_type VARCHAR,
            ltp DOUBLE,
            predicted_return DOUBLE,
            edge_score DOUBLE,
            decision VARCHAR,
            reason VARCHAR,
            features_json VARCHAR
        );
        """)
        self.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            ts TIMESTAMP,
            mode VARCHAR,
            trading_symbol VARCHAR,
            transaction_type VARCHAR,
            quantity INTEGER,
            price DOUBLE,
            groww_order_id VARCHAR,
            order_reference_id VARCHAR,
            order_status VARCHAR,
            raw_json VARCHAR
        );
        """)
        self.execute("""
        CREATE TABLE IF NOT EXISTS backtest_metrics (
            ts TIMESTAMP,
            model_path VARCHAR,
            trades INTEGER,
            win_rate DOUBLE,
            mean_return DOUBLE,
            sharpe DOUBLE,
            alpha_vs_universe DOUBLE,
            passed BOOLEAN,
            raw_json VARCHAR
        );
        """)
        for table in ("option_chain_snapshots", "quote_snapshots"):
            try:
                self.execute(f"ALTER TABLE {table} ADD COLUMN source VARCHAR")
            except Exception:
                pass
        try:
            self.execute("CREATE INDEX IF NOT EXISTS idx_chain_symbol_ts ON option_chain_snapshots(trading_symbol, ts);")
            self.execute("CREATE INDEX IF NOT EXISTS idx_quote_symbol_ts ON quote_snapshots(trading_symbol, ts);")
        except Exception:
            pass
        if self.backend == "sqlite":
            self.con.commit()

    def _table_columns(self, table: str) -> list[str]:
        if self.backend == "duckdb":
            rows = self.con.execute(f"PRAGMA table_info('{table}')").fetchall()
            return [r[1] for r in rows]
        rows = self.con.execute(f"PRAGMA table_info({table})").fetchall()
        return [r[1] for r in rows]

    def append_df(self, table: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        cols = self._table_columns(table)
        aligned = df.copy()
        for c in cols:
            if c not in aligned.columns:
                aligned[c] = None
        aligned = aligned[cols]
        if self.backend == "duckdb":
            self.con.register("_append_df", aligned)
            col_list = ", ".join(cols)
            self.con.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM _append_df")
            self.con.unregister("_append_df")
        else:
            aligned.to_sql(table, self.con, if_exists="append", index=False)
            self.con.commit()
        return len(aligned)

    def query_df(self, sql: str, params: tuple | list | None = None) -> pd.DataFrame:
        if self.backend == "duckdb":
            if params is None:
                return self.con.execute(sql).fetchdf()
            return self.con.execute(sql, params).fetchdf()
        return pd.read_sql_query(sql, self.con, params=params)

    def close(self) -> None:
        self.con.close()
