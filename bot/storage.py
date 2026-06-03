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
        self._maybe_import_sqlite_fallback()

    def execute(self, sql: str, params: tuple | list | None = None):
        if params is None:
            return self.con.execute(sql)
        return self.con.execute(sql, params)

    def init_schema(self) -> None:
        self.execute("""
        CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            ts TIMESTAMP,
            trade_date DATE,
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
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
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
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
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
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
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
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
        CREATE TABLE IF NOT EXISTS managed_positions (
            ts TIMESTAMP,
            updated_ts TIMESTAMP,
            mode VARCHAR,
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
            trading_symbol VARCHAR,
            entry_side VARCHAR,
            exit_side VARCHAR,
            quantity INTEGER,
            entry_price DOUBLE,
            tp_pct DOUBLE,
            sl_pct DOUBLE,
            target_price DOUBLE,
            stop_price DOUBLE,
            tick_size DOUBLE,
            product VARCHAR,
            entry_order_id VARCHAR,
            entry_order_reference_id VARCHAR,
            status VARCHAR,
            exit_reason VARCHAR,
            exit_order_id VARCHAR,
            exit_order_reference_id VARCHAR,
            exit_order_status VARCHAR,
            raw_json VARCHAR
        );
        """)
        self.execute("""
        CREATE TABLE IF NOT EXISTS backtest_metrics (
            ts TIMESTAMP,
            asset_id VARCHAR,
            underlying VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
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
        table_columns = {
            "option_chain_snapshots": ["asset_id", "underlying", "exchange", "segment", "source"],
            "quote_snapshots": ["asset_id", "underlying", "exchange", "segment", "source"],
            "model_signals": ["asset_id", "underlying", "exchange", "segment"],
            "orders": ["asset_id", "underlying", "exchange", "segment"],
            "managed_positions": ["asset_id", "underlying", "exchange", "segment"],
            "backtest_metrics": ["asset_id", "underlying", "exchange", "segment"],
        }
        for table, columns in table_columns.items():
            for column in columns:
                try:
                    self.execute(f"ALTER TABLE {table} ADD COLUMN {column} VARCHAR")
                except Exception:
                    pass
        for table in table_columns:
            try:
                self.execute(
                    f"""
                    UPDATE {table}
                    SET asset_id = coalesce(asset_id, 'nifty'),
                        underlying = coalesce(underlying, 'NIFTY'),
                        exchange = coalesce(exchange, 'NSE'),
                        segment = coalesce(segment, 'FNO')
                    WHERE asset_id IS NULL OR underlying IS NULL OR exchange IS NULL OR segment IS NULL
                    """
                )
            except Exception:
                pass
        try:
            self.execute("CREATE INDEX IF NOT EXISTS idx_chain_symbol_ts ON option_chain_snapshots(trading_symbol, ts);")
            self.execute("CREATE INDEX IF NOT EXISTS idx_quote_symbol_ts ON quote_snapshots(trading_symbol, ts);")
            self.execute("CREATE INDEX IF NOT EXISTS idx_chain_asset_symbol_ts ON option_chain_snapshots(asset_id, trading_symbol, ts);")
            self.execute("CREATE INDEX IF NOT EXISTS idx_quote_asset_symbol_ts ON quote_snapshots(asset_id, trading_symbol, ts);")
        except Exception:
            pass
        if self.backend == "sqlite":
            self.con.commit()

    def _table_row_count(self, table: str) -> int:
        try:
            row = self.query_df(f"SELECT count(*) AS rows FROM {table}").iloc[0]
            return int(row.get("rows") or 0)
        except Exception:
            return 0

    def _maybe_import_sqlite_fallback(self) -> None:
        if self.backend != "duckdb" or self.db_path.suffix != ".duckdb":
            return
        sqlite_path = self.db_path.with_suffix(".sqlite")
        if not sqlite_path.exists() or sqlite_path.resolve() == self.db_path.resolve():
            return
        tables = [
            "option_chain_snapshots",
            "quote_snapshots",
            "model_signals",
            "orders",
            "backtest_metrics",
        ]
        if any(self._table_row_count(table) > 0 for table in tables):
            return

        sqlite_con = sqlite3.connect(str(sqlite_path))
        try:
            existing = {
                row[0]
                for row in sqlite_con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            for table in tables:
                if table not in existing:
                    continue
                for chunk in pd.read_sql_query(f"SELECT * FROM {table}", sqlite_con, chunksize=50000):
                    self.append_df(table, chunk)
        finally:
            sqlite_con.close()

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
