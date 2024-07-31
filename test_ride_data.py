import unittest
# from unittest import mock
from unittest.mock import create_autospec, patch
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
            conn = app_db.connect_db(app_db.db_path)

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


if __name__ == "__main__":
    unittest.main()
