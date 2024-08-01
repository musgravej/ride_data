import unittest

# from unittest import mock
from unittest.mock import patch
import sqlite3
import os
import tempfile

from ride_data import App, AppDB


class TestAppDB(unittest.TestCase):

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

    def test_db_stats_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_db = os.path.join(temp_dir, "test.db")
            app_db = AppDB(temp_db)
            sqlite3.connect(temp_db).execute("DROP TABLE `ride_data`;")
            with self.assertRaises(Exception):
                self.assertEqual(app_db.db_stats(), {})


if __name__ == "__main__":
    unittest.main()
