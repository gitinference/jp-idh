from CensusForge import CensusAPI
import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path


class PRidh:
    def __init__(self, saving_dir: str):
        self.conn = duckdb.connect()
        self.saving_dir = saving_dir

    def pull_pumspr(self):
        for _year in range(2012, datetime.now().year):
            file_path = Path(f"{self.saving_dir}processed/pumspr-{_year}.parquet")

            if not file_path.exists() and _year != 2020:
                pumspr = CensusAPI().query(
                    dataset="acs-acs1-pumspr",
                    params_list=["AGEP", "SCH", "SCHL", "HINCP", "PWGTP"],
                    year=_year,
                )
                pumspr = pumspr.with_columns(year=pl.lit(_year))
                pumspr.write_parquet(file=file_path)
        return self.conn.execute(
            f"SELECT * FROM '{self.saving_dir}processed/pumspr-*.parquet';"
        ).pl()
