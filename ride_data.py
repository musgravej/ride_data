import os

# import csv
import sqlite3
import sys
from sqlite3 import Connection
from typing import Optional

import pandas as pd
import pendulum

DB_NAME = "ridedb.db"


class AppDB:
    """
    class for managing sqlite database
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

    def db_stats(self) -> dict:
        """
        Returns a string with some database statistics
        """
        stats = {}
        with self.connect_db(self.db_path) as conn:
            try:
                stats["row_count"] = (
                    conn.execute("SELECT COUNT(*) AS cnt FROM ride_data;")
                    .fetchone()
                    .get("cnt", 0)
                )
                stats["file_count"] = (
                    conn.execute(
                        "WITH qry AS (SELECT FileName FROM ride_data GROUP BY FileName) "
                        "SELECT count(*) AS count FROM qry;"
                    )
                    .fetchone()
                    .get("count", 0)
                )
                stats["date_qry"] = (
                    "WITH qry1 AS (select datetime(CheckoutDateLocal||' '||CheckoutTimeLocal) AS return_dt "
                    "FROM ride_data ) SELECT MIN(return_dt) AS min_return , MAX(return_dt) AS max_return FROM qry1;"
                )
                date_rslt = conn.execute(date_qry).fetchone()
                stats["min_date"] = date_rslt["min_return"]
                stats["max_date"] = date_rslt["max_return"]
            except Exception as e:
                print(f"db stats error | {e}")

        conn.close()
        return stats

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
                df["ReturnDateTime"] = (
                    df["ReturnDateLocal"] + " " + df["ReturnTimeLocal"]
                )
                df["CheckoutDateTime"] = (
                    df["CheckoutDateLocal"] + " " + df["CheckoutTimeLocal"]
                )
                df_cols = df.columns.to_list()

                for row in df.itertuples():
                    placeholders = ", ".join("?" * len(df_cols))
                    values = [
                        int(x) if isinstance(x, bool) else x
                        for x in (row.__getattribute__(each) for each in df_cols)
                    ]
                    sql = "REPLACE INTO ride_data ({}) VALUES ({});".format(
                        ", ".join(df_cols), placeholders
                    )
                    conn.execute(sql, values)
                conn.commit()
            except Exception as e:
                print(f"import report failure | {e}")
        conn.close()

    def create_temp_table(self, table_name: str, sql: str) -> None:
        """
        create view if not exists tmp_table as select * from ride_data where `ReturnDateLocal` > '2024-05-15';
        """
        pass


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

    def exit_app(self, exit_code: int = 0) -> None:
        print("Exiting app")
        sys.exit(exit_code)

    def show_main_menu(self) -> None:
        option_map = {
            "1": {"function": self.show_db_menu, "description": "Database Actions"},
            "2": {"function": self.show_report_menu, "description": "Report Actions"},
            "3": {"function": self.exit_app, "description": "Quit"},
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())
        stats = self.db.db_stats()
        # print(stats)

        user_choice = input(
            f"\nMain Menu:\n{'=' * 10}\nPick action:\n" + "\n".join(options) + "\n"
        )
        while user_choice not in option_map.keys():
            self.show_main_menu()
        option_map[user_choice]["function"]()

    def set_date_range(self) -> None:
        pass

    def show_db_menu(self) -> None:
        option_map = {
            "1": {
                "function": self.import_report_file,
                "description": "Import report csv file",
            },
            "2": {
                "function": self.set_date_range,
                "desciption": "Set temporary date range",
            },
            "3": {
                "function": self.show_main_menu,
                "description": "Return to Main menu",
            },
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())
        user_choice = input(
            f"\nDatabase Menu:\n{'=' * 14}\nPick action:\n" + "\n".join(options) + "\n"
        )
        while user_choice not in option_map.keys():
            self.show_db_menu()
        option_map[user_choice]["function"]()
        self.show_db_menu()

    def import_report_file(self) -> None:
        file_path = input("Report CSV file path: ")
        self.db.import_report_to_db(file_path)

    def show_report_menu(self) -> None:
        option_map = {
            "1": {
                "function": self.show_main_menu,
                "description": "Return to Main menu",
            },
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())

        user_choice = input(
            f"\nReport Menu:\n{'=' * 12}\nPick action:\n" + "\n".join(options) + "\n"
        )
        while user_choice not in option_map.keys():
            self.show_report_menu()
        option_map[user_choice]["function"]()
        self.show_report_menu()


def run():
    app = App()
    if not app.init_app():
        app.exit_app(1)

    app.show_main_menu()
    pass


if __name__ == "__main__":
    run()
