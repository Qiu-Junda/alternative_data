import os
import utils
import sqlite3


class Fundamentals:
    def __init__(self):
        self.db_name = 'fundamentals.db'
        self.path = os.path.join(utils.my_path, self.db_name)
        self.conn = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.cursor = self.conn.cursor()
