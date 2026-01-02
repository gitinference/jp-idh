from CensusForge import CensusAPI
import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path
import world_bank_data as wb
import logging
from requests.exceptions import HTTPError


class PRidh:
    def __init__(self, saving_dir: str):
        self.conn = duckdb.connect()
        self.saving_dir = saving_dir

    def pull_pumspr(self):
        for _year in range(2012, datetime.now().year - 1):
            file_path = Path(f"{self.saving_dir}processed/pumspr-{_year}.parquet")

            if not file_path.exists() and _year != 2020:
                pumspr = CensusAPI().query(
                    dataset="acs-acs1-pumspr",
                    params_list=["AGEP", "SCH", "SCHL", "HINCP", "PWGTP"],
                    year=_year,
                    geography="state",
                )
                pumspr = pl.DataFrame(pumspr)
                pumspr = pumspr.with_columns(year=pl.lit(_year))
                pumspr.write_parquet(file=file_path)
        return self.conn.execute(
            f"SELECT * FROM '{self.saving_dir}processed/pumspr-*.parquet';"
        ).pl()

    def pull_wb(self):
        for _year in range(2012, 2024):
            file_path = Path(f"{self.saving_dir}processed/wb-{_year}.parquet")
            if not file_path.exists() and _year != 2020:
                try:
                    capita = wb.get_series(
                        "NY.GNP.PCAP.PP.CD",
                        country="PR",
                        simplify_index=True,
                        date=str(_year),
                    )
                    constant = wb.get_series(
                        "NY.GNP.PCAP.PP.KD",
                        country="PR",
                        simplify_index=True,
                        date=str(_year),
                    )
                    life_exp = wb.get_series(
                        "SP.DYN.LE00.IN",
                        country="PR",
                        simplify_index=True,
                        date=str(_year),
                    )

                    df_gni = pl.DataFrame(
                        [
                            pl.Series("year", [_year], dtype=pl.Int64),
                            pl.Series("capita", [capita], dtype=pl.Float64),
                            pl.Series("constant", [constant], dtype=pl.Float64),
                            pl.Series("life_exp", [life_exp], dtype=pl.Float64),
                        ]
                    )
                    df_gni.write_parquet(file_path)
                    # Logging
                    logging.info(
                        f"Successfully inserted world bank data for year {_year}"
                    )
                except HTTPError:
                    logging.warning(
                        f"Could not insert world bank data for year {_year}"
                    )

            else:
                logging.info(f"Data for year {_year} already exists in gnitable")
                continue
        return self.conn.execute(
            f"SELECT * FROM '{self.saving_dir}processed/wb-*.parquet';"
        ).pl()
