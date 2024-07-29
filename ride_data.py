import pendulum

# import csv
import sqlite3
from sqlite3 import Connection
import os
import sys
import pandas as pd

from typing import Optional

DB_NAME = "ridedb.db"


class AppDB:
    """
    class for managing sqlite database
    # TODO add decorator for handling db connection
    """

    def __init__(self, path: str) -> None:
        self.db_path = path

    @staticmethod
    def csv_fields() -> list:
        return [
            "TripId",
            "UserProgramName",
            "UserId",
            "UserRole",
            "UserCity",
            "UserState",
            "UserZip",
            "UserCountry",
            "MembershipType",
            "Bike",
            "BikeType",
            "CheckoutKioskName",
            "ReturnKioskName",
            "DurationMins",
            "AdjustedDurationMins",
            "UsageFee",
            "AdjustmentFlag",
            "Distance",
            "EstimatedCarbonOffset",
            "EstimatedCaloriesBurned",
            "CheckoutDateLocal",
            "ReturnDateLocal",
            "CheckoutTimeLocal",
            "ReturnTimeLocal",
            "TripOver30Mins",
            "LocalProgramFlag",
            "TripRouteCategory",
            "TripProgramName",
        ]

    @staticmethod
    def create_db_schema() -> str:
        return (
            "CREATE TABLE IF NOT EXISTS ride_data ("
            "`TripId` INTEGER  PRIMARY KEY,"
            "`UserProgramName` TEXT,"
            "`UserId` INTEGER,"
            "`UserRole` TEXT,"
            "`UserCity` TEXT,"
            "`UserState` TEXT,"
            "`UserZip` TEXT,"
            "`UserCountry` TEXT,"
            "`MembershipType` TEXT,"
            "`Bike` TEXT,"
            "`BikeType` TEXT,"
            "`CheckoutKioskName` TEXT,"
            "`ReturnKioskName` TEXT,"
            "`DurationMins` REAL,"
            "`AdjustedDurationMins` REAL,"
            "`UsageFee` REAL,"
            "`AdjustmentFlag` TEXT,"
            "`Distance` REAL,"
            "`EstimatedCarbonOffset` REAL,"
            "`EstimatedCaloriesBurned` REAL,"
            "`CheckoutDateLocal` TEXT,"
            "`ReturnDateLocal` TEXT,"
            "`CheckoutTimeLocal` TEXT,"
            "`ReturnTimeLocal` TEXT,"
            "`TripOver30Mins` TEXT,"
            "`LocalProgramFlag` TEXT,"
            "`TripRouteCategory` TEXT,"
            "`TripProgramName` TEXT,"
            "`FileName` TEXT,"
            "`ImportDateTime` TEXT,"
            "`CheckoutDateTime` TEXT,"
            "`ReturnDateTime` TEXT"
            ");"
        )

    @staticmethod
    def df_dtype() -> dict:
        return {
            "TripId": "int",
            "UserProgramName": "object",
            "UserId": "int",
            "UserRole": "object",
            "UserCity": "object",
            "UserState": "object",
            "UserZip": "object",
            "UserCountry": "object",
            "MembershipType": "object",
            "Bike": "object",
            "BikeType": "object",
            "CheckoutKioskName": "object",
            "ReturnKioskName": "object",
            "DurationMins": "float",
            "AdjustedDurationMins": "float",
            "UsageFee": "float",
            "AdjustmentFlag": "object",
            "Distance": "float",
            "EstimatedCarbonOffset": "float",
            "EstimatedCaloriesBurned": "float",
            "CheckoutDateLocal": "object",
            "ReturnDateLocal": "object",
            "CheckoutTimeLocal": "object",
            "ReturnTimeLocal": "object",
            "TripOver30Mins": "object",
            "LocalProgramFlag": "object",
            "TripRouteCategory": "object",
            "TripProgramName": "object",
        }

    @staticmethod
    def dict_factory(cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    def connect_db(self, path: str) -> Connection:
        conn = sqlite3.connect(path)
        conn.row_factory = AppDB.dict_factory
        return conn

    def init_db(self) -> None:
        if os.path.exists(self.db_path):
            try:
                conn = self.connect_db(self.db_path)
                if self.db_table_exists(conn, "ride_data"):
                    conn.close()
                    return
            except Exception as e:
                raise Exception(f"connection failure | {e}")

        print("intializing database")
        try:
            conn = self.connect_db(self.db_path)
            conn.execute(self.create_db_schema())
            conn.commit()
            conn.close()
        except Exception as e:
            raise Exception(f"database creation error | {e}")

    def db_table_exists(self, conn: Connection, table_name: str) -> bool:
        try:
            qry = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
            return conn.execute(qry, (table_name,)).fetchone() is not None
        except sqlite3.OperationalError:
            return False

    def db_stats_to_string(self, conn: Connection) -> str:
        try:
            row_count = conn.execute("SELECT COUNT(*) AS cnt FROM ride_data;").fetchone().get("cnt", 0)
            file_count = (
                conn.execute(
                    "WITH qry AS (SELECT FileName FROM ride_data GROUP BY FileName) select count(*) AS count FROM qry;"
                )
                .fetchone()
                .get("count", 0)
            )
            date_qry = (
                "WITH qry1 AS (select datetime(CheckoutDateLocal||' '||CheckoutTimeLocal) AS return_dt FROM ride_data )"
                " SELECT MIN(return_dt) AS min_return , MAX(return_dt) AS max_return FROM qry1;"
            )
            date_rslt = conn.execute(date_qry).fetchone()
            return (
                f"DB Rows: {row_count}\nFile Count: {file_count}\n"
                f"Min Checkout Date: {date_rslt["min_return"]}\nMax Checkout Date: {date_rslt["max_return"]}"
            )
        except Exception as e:
            return f"error | {e}"

    def import_report_to_db(self, report_path: str) -> None:
        """
        Import a csv file, of correct format to db
        """
        with self.connect_db(self.db_path) as conn:
            try:
                conn = self.connect_db(self.db_path)
                filename = os.path.split(report_path)[1]
                print(f"importing report: '{filename}'")

                # pre-sql data processing
                df = pd.read_csv(report_path, dtype=self.df_dtype())
                df = df.fillna("")
                df["FileName"] = filename
                df["ImportDateTime"] = pendulum.now().to_datetime_string()
                df["ReturnDateTime"] = df["ReturnDateLocal"] + " " + df["ReturnTimeLocal"]
                df["CheckoutDateTime"] = df["CheckoutDateLocal"] + " " + df["CheckoutTimeLocal"]
                df_cols = df.columns.to_list()

                for row in df.itertuples():
                    placeholders = ', '.join('?' * len(df_cols))
                    values = [
                        int(x) if isinstance(x, bool) else x for x in (row.__getattribute__(each) for each in df_cols)
                    ]
                    sql = "REPLACE INTO ride_data ({}) VALUES ({});".format(", ".join(df_cols), placeholders)
                    conn.execute(sql, values)
                conn.commit()
            except Exception as e:
                print(f"import report failure | {e}")
        conn.close()


class App:
    def __init__(self, db_name: Optional[str] = None) -> None:
        self.db_name = db_name or DB_NAME
        self.db = AppDB(self.db_name)
        self.session_name_string = pendulum.now().format("YYYY-MM-DD_HH-mm-ss")

    def init_app(self) -> bool:
        """
        Initialize app, if fails, return false
        """
        try:
            self.db.init_db()
        except Exception as e:
            print(f"failed to initialize app | {e}")
            return False
        return True

    def show_main_menu(self) -> None:
        # stats = self.db.db_stats_to_string()
        pass

    def show_db_menu(self) -> None:
        pass

    def show_report_menu(self) -> None:
        pass

    def session_to_string(self) -> str:
        return ""


def run():
    app = App()
    # app = App("ride_db.db")
    if not app.init_app():
        sys.exit(1)

    # app.db.import_report_to_db("./BCycle_45_DesMoinesBCycle_20240501_20240531.csv")
    # app.db.import_report_to_db("./BCycle_45_DesMoinesBCycle_20240601_20240630.csv")
    pass


if __name__ == "__main__":
    run()
