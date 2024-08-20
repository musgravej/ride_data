# import pathlib
import argparse
import os
import re

# import csv
import sqlite3
import sys
from sqlite3 import Connection
from typing import Optional

import pandas as pd
import pendulum
from pendulum.parsing.exceptions import ParserError

# from pendulum.datetime import DateTime

DB_NAME = "ridedb.db"


class AppDB:
    """
    class for managing sqlite database
    """

    def __init__(self, path: str, table_name: Optional[str] = None) -> None:
        self.db_path = path
        self.start_range = None
        self.end_range = None
        self.table_name = table_name or "ride_data"
        self.temp_view_name = None

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
        # os path must exist, in :memory: not supported
        if os.path.exists(self.db_path):
            try:
                conn = self.connect_db(self.db_path)
                if self.db_table_exists(conn, self.table_name):
                    conn.close()
                    return
            except Exception as e:
                raise Exception(f"connection failure | {e}")

        print("# Intializing database")
        try:
            conn = self.connect_db(self.db_path)
            conn.execute(self.create_db_schema())
            conn.commit()
            conn.close()
        except Exception as e:
            raise Exception(f"database creation error | {e}")

    @staticmethod
    def db_table_exists(conn: Connection, table_name: str) -> bool:
        try:
            qry = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
            return conn.execute(qry, (table_name,)).fetchone() is not None
        except sqlite3.OperationalError:
            return False

    # TODO test
    def drop_temp_tables(self):
        """
        Temp tables is saved as views
        """
        with self.connect_db(self.db_path) as conn:
            if views := conn.execute("SELECT name from sqlite_master where type = 'view';").fetchall():
                [conn.execute(f"DROP VIEW {each['name']};") for each in views]

    def get_current_table_name(self) -> str:
        return self.temp_view_name or self.table_name

    # TODO test custom table name
    def db_stats(self, table_name: Optional[str] = None) -> list:
        """
        Returns a dictionary with some database statistics
        """
        stats = []
        table_name = self.get_current_table_name()
        with self.connect_db(self.db_path) as conn:
            try:
                stats.append(
                    {
                        "name": "database count",
                        "value": (
                            conn.execute(
                                "SELECT COUNT(*) AS cnt FROM {};".format(
                                    table_name,
                                )
                            )
                            .fetchone()
                            .get("cnt", 0)
                        ),
                    }
                )
                stats.append(
                    {
                        "name": "file count",
                        "value": (
                            conn.execute(
                                "WITH qry AS (SELECT FileName FROM {} GROUP BY FileName) "
                                "SELECT count(*) AS count FROM qry;".format(
                                    table_name,
                                )
                            )
                            .fetchone()
                            .get("count", 0)
                        ),
                    }
                )
                date_qry = (
                    "WITH qry1 AS (select datetime(CheckoutDateLocal||' '||CheckoutTimeLocal) AS return_dt "
                    "FROM {}) SELECT MIN(return_dt) AS min_return , MAX(return_dt) AS max_return FROM qry1;".format(
                        table_name,
                    )
                )
                date_rslt = conn.execute(date_qry).fetchone()
                stats.append({"name": "min date", "value": date_rslt["min_return"]})
                stats.append({"name": "max date", "value": date_rslt["max_return"]})
            except Exception as e:
                print(f"\n# DB stats error | {e}")

        conn.close()
        return stats

    def import_report_to_db(self, report_path: str) -> None:
        """
        Import a csv file, of correct format to db
        """
        with self.connect_db(self.db_path) as conn:
            try:
                # conn = self.connect_db(self.db_path)
                filename = os.path.split(report_path)[1]
                print(f"# Importing report: '{filename}'")

                # pre-sql data processing
                df = pd.read_csv(report_path, dtype=self.df_dtype())
                df = df.fillna("")
                df["FileName"] = filename
                df["ImportDateTime"] = pendulum.now().to_datetime_string()
                df["ReturnDateTime"] = df["ReturnDateLocal"] + " " + df["ReturnTimeLocal"]
                df["CheckoutDateTime"] = df["CheckoutDateLocal"] + " " + df["CheckoutTimeLocal"]
                df_cols = df.columns.to_list()

                for row in df.itertuples():
                    placeholders = ", ".join("?" * len(df_cols))
                    values = [
                        int(x) if isinstance(x, bool) else x for x in (row.__getattribute__(each) for each in df_cols)
                    ]
                    sql = "REPLACE INTO ride_data ({}) VALUES ({});".format(", ".join(df_cols), placeholders)
                    conn.execute(sql, values)
                conn.commit()
            except Exception as e:
                print(f"\n# Import report failure | {e}")
        conn.close()

    def create_temporary_view(self, view_name: str, sql: str) -> None:
        with self.connect_db(self.db_path) as conn:
            try:
                # conn = self.connect_db(self.db_path)
                # Drop table first
                conn.execute(f"DROP VIEW IF EXISTS {view_name};")
                conn.execute(sql)
                conn.commit()
                self.temp_view_name = view_name

            except Exception as e:
                print(f"\n# Temp view failure | {e}")

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
            self.db.drop_temp_tables()
        except Exception as e:
            print(f"failed to initialize app | {e}")
            return False
        return True

    def exit_app(self, exit_code: int = 0) -> None:
        print("Exiting app")
        sys.exit(exit_code)

    def print_stats(self, stats_list: list) -> None:
        if self.db.temp_view_name:
            print("\n## TEMPORARY TABLE ##")
        print_strings = [f"{each['name']}: {each['value']}" for each in stats_list]
        print(*print_strings, sep="\n")

    def show_main_menu(self) -> None:
        option_map = {
            "1": {"function": self.show_db_menu, "description": "Database Actions"},
            "2": {"function": self.show_report_menu, "description": "Report Actions"},
            "3": {"function": self.exit_app, "description": "Quit"},
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())
        self.print_stats(self.db.db_stats())

        user_choice = input(f"\nMain Menu:\n{'=' * 10}\nPick action:\n" + "\n".join(options) + "\n")
        while user_choice not in option_map.keys():
            self.show_main_menu()
        option_map[user_choice]["function"]()

    @staticmethod
    def valid_date_parse(date_string: str) -> bool:
        try:
            pendulum.parse(date_string)
            return True
        except (ValueError, ParserError):
            print(f"\n# Invalid date: {date_string}")
            return False

    @staticmethod
    def get_start_end_date_range() -> tuple[str, str] | tuple[None, None]:
        print("\nEnter date as string (ex: 2022-04-24)")
        start_date = input("Start Date: ")
        if not App.valid_date_parse(start_date):
            return None, None

        end_date = input(f"End Date (or {pendulum.now().to_date_string()}): ")
        end_date = end_date or pendulum.now().to_datetime_string()
        if not App.valid_date_parse(end_date):
            return None, None

        return (
            pendulum.parse(start_date).start_of("day").to_datetime_string(),
            pendulum.parse(end_date).end_of("day").to_datetime_string()
        )

    def set_temp_date_range(self) -> None:
        start_date_dt, end_date_dt = App.get_start_end_date_range()
        if start_date_dt is None or end_date_dt is None:
            return

        sql = (
            "CREATE VIEW temp AS SELECT * FROM ride_data "
            f"WHERE CheckoutDateTime >= '{start_date_dt}' AND CheckoutDateTime <= '{end_date_dt}';"
        )

        self.db.create_temporary_view("temp", sql)

    def clear_date_range(self):
        self.db.drop_temp_tables()
        self.db.temp_view_name = None

    @staticmethod
    def choice_picker(
        input_message: str,
        choices: dict,
        pre_input_message: Optional[str] = None,
        post_input_message: Optional[str] = None,
    ) -> str:
        if pre_input_message is not None:
            print(pre_input_message)

        user_choice = input(input_message)
        while user_choice not in choices.keys():
            App.choice_picker(input_message, choices, pre_input_message, post_input_message)

        if post_input_message is not None:
            print(post_input_message)

        return user_choice

    # TODO
    def drop_rows_by_filename(self):
        print("\nDrop rows by filename:")
        table_name = self.db.get_current_table_name()
        filenames = {}
        with self.db.connect_db(self.db.db_path) as conn:
            try:
                results = conn.execute(
                    f"SELECT `FileName` from {table_name} GROUP BY `FileName` ORDER BY `FileName`;"
                ).fetchmany()
                filenames = {_idx.__str__(): row["FileName"] for _idx, row in enumerate(results, 1)}
                filenames[(len(filenames) + 1).__str__()] = "Cancel"
                pre_input = "\n".join(f"{n}: {f}" for n, f in filenames.items())
                choice = App.choice_picker("Pick filename by number: ", filenames, pre_input_message=pre_input)

                filename_choice = filenames.get(choice)
                if filename_choice == "Cancel":
                    conn.close()
                    return

                conn.execute(f"DELETE FROM {table_name} WHERE `FileName` = '{filename_choice}';")
                conn.commit()

            except Exception as e:
                print(f"\n# Drop rows by filename error | {e}")
        conn.close()

    # TODO test
    def drop_rows_by_date_range(self):
        print("\nDrop rows by date range:")
        table_name = self.db.get_current_table_name()
        with self.db.connect_db(self.db.db_path) as conn:
            try:
                start_date_dt, end_date_dt = App.get_start_end_date_range()
                if start_date_dt is None or end_date_dt is None:
                    return

                sql = (
                    f"DELETE FROM `{table_name}` WHERE CheckoutDateTime >= '{start_date_dt}' "
                    f"AND ReturnDateTime <= '{end_date_dt}';"
                )
                conn.execute(sql)
                conn.commit()
            except Exception as e:
                print(f"\n# Drop rows by date range error | {e}")
        conn.close()

    def show_db_menu(self) -> None:
        option_map = {
            "1": {
                "function": self.import_report_file,
                "description": "Import report csv file",
            },
            "2": {
                "function": self.set_temp_date_range,
                "description": "Set temporary date range",
            },
            "3": {
                "function": self.clear_date_range,
                "description": "Clear temporary date range",
            },
            "4": {
                "function": self.drop_rows_by_date_range,
                "description": "Drop table rows by date range",
            },
            "5": {
                "function": self.drop_rows_by_filename,
                "description": "Drop table rows by file name",
            },
            "6": {
                "function": self.show_main_menu,
                "description": "Return to Main menu",
            },
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())
        user_choice = input(f"\nDatabase Menu:\n{'=' * 14}\nPick action:\n" + "\n".join(options) + "\n")
        while user_choice not in option_map.keys():
            self.show_db_menu()
        option_map[user_choice]["function"]()
        self.show_db_menu()

    def import_report_file(self) -> None:
        # validate file path
        file_path = input("Report CSV file path: ")
        if not os.path.exists(file_path):
            print(f"\n# File path error | {file_path}")
            return

        self.db.import_report_to_db(file_path)

    def show_report_menu(self) -> None:
        option_map = {
            "1": {
                "function": self.show_main_menu,
                "description": "Return to Main menu",
            },
        }
        options = list(f"{k}: {v['description']}" for k, v in option_map.items())

        user_choice = input(f"\nReport Menu:\n{'=' * 12}\nPick action:\n" + "\n".join(options) + "\n")
        while user_choice not in option_map.keys():
            self.show_report_menu()
        option_map[user_choice]["function"]()
        self.show_report_menu()


def db_path_type(value: Optional[str] = None) -> str | None:
    if value is not None and not re.match(r"^.*\.(db)$", value):
        raise argparse.ArgumentTypeError("invalid db path name")
    return value


def app_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ride data processor")
    parser.add_argument(
        "--db-path",
        action="store",
        type=db_path_type,
        help="initialize app with custom database path name",
    )
    return parser


def run():
    args = app_args()
    parsed_args = args.parse_args()

    app = App(parsed_args.db_path)
    if not app.init_app():
        app.exit_app(1)

    app.show_main_menu()
    pass


if __name__ == "__main__":
    run()
