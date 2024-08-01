import csv
import io
import sys
import unittest

# from unittest import mock
from unittest.mock import patch
import sqlite3
import os
import tempfile

from ride_data import App, AppDB


class TestAppDB(unittest.TestCase):
    def sample_csv_lines(self) -> list:
        return [
            (
                "TripId,UserProgramName,UserId,UserRole,UserCity,UserState,UserZip,UserCountry,MembershipType,Bike,"
                "BikeType,CheckoutKioskName,ReturnKioskName,DurationMins,AdjustedDurationMins,UsageFee,AdjustmentFlag,"
                "Distance,EstimatedCarbonOffset,EstimatedCaloriesBurned,CheckoutDateLocal,ReturnDateLocal,"
                "CheckoutTimeLocal,ReturnTimeLocal,TripOver30Mins,LocalProgramFlag,TripRouteCategory,TripProgramName\n"
            ),
            (
                "33567793,Des Moines BCycle,2395732,Maintenance,,,,UNITED STATES,,21865,Standard,Lauridsen Skatepark,"
                "Lauridsen Skatepark,0,0,0,N,.0,.0,0,2024-06-02,2024-06-02,16:06:24,16:06:32,N,Y,Round Trip,"
                "Des Moines BCycle\n"
            ),
            (
                "33567803,Des Moines BCycle,2395732,Maintenance,,,,UNITED STATES,,11434,Standard,Lauridsen Skatepark,"
                "Lauridsen Skatepark,0,0,0,N,.0,.0,0,2024-06-02,2024-06-02,16:07:27,16:07:35,N,Y,Round Trip,"
                "Des Moines BCycle\n"
            ),
        ]

    def test_connect_db(self):
        app_db = AppDB(":memory:")
        conn = app_db.connect_db(app_db.db_path)
        self.assertIsInstance(conn, sqlite3.Connection)

    def test_connect_db_failed(self):
        with self.assertRaises(sqlite3.OperationalError):
            app_db = AppDB("../foo/foo.db")
            app_db.connect_db(app_db.db_path)

    def test_init_db_table_exists(self):
        # setup test db
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            conn = sqlite3.connect(temp_db)
            conn.execute("CREATE table `ride_data` (`foo` str);")
            conn.commit()
            conn.close()

            app_db = AppDB(temp_db)
            app_db.init_db()

    def test_init_db_table_missing(self):
        # setup test db
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.init_db()

    def test_init_db_missing(self):
        with self.assertRaises(Exception):
            app_db = AppDB("../foo/foo.db")
            app_db.init_db()

    def test_init_db_init_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            conn = sqlite3.connect(temp_db)
            conn.execute("CREATE table `ride_data` (`foo` str);")
            conn.commit()
            conn.close()

            app_db = AppDB(temp_db)
            app_db.init_db()

    def test_init_db_create_schema(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.init_db()

            conn = app_db.connect_db(temp_db)
            self.assertTrue(app_db.db_table_exists(conn, "ride_data"))

    @patch("ride_data.AppDB.create_db_schema")
    def test_init_db_create_schema_error(self, func):
        func.return_value = "CREATE TABLE `foo` foo bar;"
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            with self.assertRaises(Exception):
                app_db.init_db()

    def test_db_table_exists(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE `foo` (bar str);")
        self.assertTrue(AppDB.db_table_exists(conn, "foo"))

    def test_db_table_exists_false(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("create table `foo` (bar str);")
        self.assertFalse(AppDB.db_table_exists(conn, "bar"))

    def test_db_stats(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            conn = sqlite3.connect(temp_db)
            conn.execute(AppDB.create_db_schema())
            sql = "INSERT INTO ride_data (FileName, CheckoutDateLocal, CheckoutTimeLocal) VALUES (?, ?, ?)"
            values = [("foo.csv", "2024-04-01", "12:00:00"), ("bar.csv", "2024-05-01", "23:00:00")]
            conn.executemany(sql, values)
            conn.commit()

            app_db = AppDB(temp_db)
            stats = {
                "file_count": 2,
                "max_date": "2024-05-01 23:00:00",
                "min_date": "2024-04-01 12:00:00",
                "row_count": 2,
            }
            self.assertEqual(app_db.db_stats(), stats)

    @patch("builtins.print")
    def test_db_stats_error(self, mock_print):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.init_db()
            sqlite3.connect(temp_db).execute("DROP TABLE `ride_data`;")
            self.assertEqual(app_db.db_stats(), {})
            mock_print.assert_called_with("db stats error | no such table: ride_data")

    def test_import_report_to_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # create temp csv file
            test_csv = os.path.join(temp_dir, "test.csv")
            with open(test_csv, "w") as fopen:
                fopen.writelines(self.sample_csv_lines())

            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.init_db()
            app_db.import_report_to_db(test_csv)

    @patch("builtins.print")
    def test_import_report_to_db_no_table(self, mock_print):
        with tempfile.TemporaryDirectory() as temp_dir:
            # create temp csv file
            test_csv = os.path.join(temp_dir, "test.csv")
            with open(test_csv, "w") as fopen:
                fopen.writelines(self.sample_csv_lines())

            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.import_report_to_db(test_csv)
            mock_print.assert_called_with("import report failure | no such table: ride_data")

    @patch("builtins.print")
    def test_import_report_to_db_no_report(self, mock_print):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            app_db.init_db()
            conn = sqlite3.connect(temp_db)
            app_db.import_report_to_db("foo.csv")
            mock_print.assert_called_with("import report failure | [Errno 2] No such file or directory: 'foo.csv'")


class TestApp(unittest.TestCase):

    pass

if __name__ == "__main__":
    unittest.main()
