# import pendulum
# import csv
import sqlite3
import os
# import pandas as pd


class AppDB:
    """
    class for managing sqlite database
    """

    def __init__(self) -> None:
        pass
        self.db = None
        self.init_db()

    def init_db(self):
        db_path = os.path.join(os.path.curdir, "ridedb.db")
        if os.path.exists(db_path):
            self.db = sqlite3.connect(db_path)
            return
        else:
            print("no db, initializing")

    def close_db(self):
        pass


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

        stats = self.db.db_stats_to_string()
        return True

    def show_main_menu(self) -> None:
        pass

    def show_db_menu(self) -> None:
        pass

    def show_report_menu(self) -> None:
        pass


def run():
    app = App()
    breakpoint()
    pass

    def session_to_string(self) -> str:
        return ""


if __name__ == "__main__":
    run()
