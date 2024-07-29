import pendulum

# import csv
import sqlite3
import os
import sys
import pandas as pd


class AppDB:
    """
    class for managing sqlite database
    # TODO add decorator for handling db connection
    """

    def __init__(self) -> None:
        self.conn = None

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
            "`ImportDateTime` TEXT"
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

    def connect_db(self, path: str):
        try:
            self.conn = sqlite3.connect(path)
            self.conn.row_factory = AppDB.dict_factory
        except sqlite3.DatabaseError as e:
            print(f"db connection error | {e}")

    def init_db(self) -> None:
        db_path = os.path.join(os.path.curdir, "ridedb.db")
        if os.path.exists(db_path):
            self.connect_db(db_path)
            if self.db_table_exists("ride_data"):
                return

        print("intializing database")
        self.conn = sqlite3.connect(db_path)
        schema = self.create_db_schema()
        self.conn.execute(schema)

    def db_table_exists(self, table_name: str) -> bool:
        if not isinstance(self.conn, sqlite3.Connection):
            return False

        qry = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
        try:
            return self.conn.execute(qry, (table_name,)).fetchone()
        except sqlite3.OperationalError:
            return False

    def db_stats_to_string(self) -> str:
        if not isinstance(self.conn, sqlite3.Connection):
            return ""

        try:
            row_count = self.conn.execute("SELECT COUNT(*) AS cnt FROM ride_data;").fetchone().get("cnt", 0)
            file_count = (
                self.conn.execute(
                    "WITH qry AS (SELECT FileName FROM ride_data GROUP BY FileName) select count(*) AS count FROM qry;"
                )
                .fetchone()
                .get("count", 0)
            )
            date_qry = (
                "WITH qry1 AS (select datetime(CheckoutDateLocal||' '||CheckoutTimeLocal) AS return_dt FROM ride_data )"
                " SELECT MIN(return_dt) AS min_return , MAX(return_dt) AS max_return FROM qry1;"
            )
            date_rslt = self.conn.execute(date_qry).fetchone()
            return (
                f"DB Rows: {row_count}\nFile Count: {file_count}\n"
                f"Min Checkout Date: {date_rslt["min_return"]}\nMax Checkout Date: {date_rslt["max_return"]}"
            )
        except Exception as e:
            return f"error | {e}"

    def import_report_to_db(self, report_path: str) -> bool:
        """
        Import a csv file, of correct format to db
        Returns True on success, False on failure
        """
        if not isinstance(self.conn, sqlite3.Connection):
            return False
        try:
            filename = os.path.split(report_path)[1]
            print(f"importing report: '{filename}'")
            df = pd.read_csv(report_path, dtype=self.df_dtype())
            df = df.fillna("")
            df["FileName"] = filename
            df["ImportDateTime"] = pendulum.now().to_datetime_string()
            for row in df.itertuples():
                cols = [each for each in row._fields if each != "Index"]
                placeholders = ', '.join('?' * len(cols))
                values = [int(x) if isinstance(x, bool) else x for x in (row.__getattribute__(each) for each in cols)]
                sql = "REPLACE INTO ride_data ({}) VALUES ({});".format(", ".join(cols), placeholders)
                self.conn.execute(sql, values)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"import report failure | {e}")
            return False

    def close_connection(self) -> None:
        if not isinstance(self.conn, sqlite3.Connection):
            return
        self.conn.close()


class App:
    def __init__(self) -> None:
        self.db = AppDB()
        self.session_name_string = pendulum.now().format("YYYY-MM-DD_HH-mm-ss")

    def init_app(self) -> bool:
        """
        Initialize app, if fails, return false
        """
        self.db.init_db()
        if self.db is None:
            return False

        return True

    def show_main_menu(self) -> None:
        stats = self.db.db_stats_to_string()
        pass

    def show_db_menu(self) -> None:
        pass

    def show_report_menu(self) -> None:
        pass

    def session_to_string(self) -> str:
        return ""


def run():
    app = App()
    if not app.init_app():
        print("app initialization failed")
        app.db.close_connection()
        sys.exit(1)

    app.db.import_report_to_db("./BCycle_45_DesMoinesBCycle_20240501_20240531.csv")
    app.db.close_connection()
    pass


if __name__ == "__main__":
    run()
