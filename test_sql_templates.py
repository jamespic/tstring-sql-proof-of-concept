import sqlite3
from tempfile import mktemp
import os
from sql_templates import Driver, Connection
from unittest import TestCase

class TestSqlTemplates(TestCase):
    db_file: str
    conn: Connection
    def setUp(self):
        self.db_file = mktemp()
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("CREATE TABLE test(col1 INT, col2 VARCHAR);")
                cursor.execute("INSERT INTO test(col1, col2) VALUES(1, 'Hello World')")
                cursor.execute("INSERT INTO test(col1, col2) VALUES(2, 'Goodbye')")
            finally:
                cursor.close()
        self.conn = Driver(sqlite3).connect(self.db_file)
    
    def tearDown(self):
        self.conn.close()
        os.remove(self.db_file)
        
    def test_happy_path(self):
        with self.conn.cursor() as cursor:
            cursor.execute(t"SELECT col2 FROM test WHERE col1 = {1}")
            result = cursor.fetchall()
            self.assertEqual(result, [("Hello World",)])