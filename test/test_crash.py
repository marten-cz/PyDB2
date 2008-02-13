import unittest
import DB2
import Config

import sys
wait_for_keypress = 1

if not "python_d" in sys.executable:
    sys.exit("ERROR: Must run in debug mode (build with debug flag and run with python_d.exe)")
if wait_for_keypress:
    raw_input("Press Enter when you've set up debugging")
    
class SimpleDB2Test_Crash(unittest.TestCase):
    def setUp(self):
        self.db = DB2.connect(**Config.ConnDict)
        self.tableName = 'PYDB2TEST_0'
        self.cs = self.db.cursor()

    def tearDown(self):
        self.cs.close()
        self.db.close()

    def _createTable(self):
        self.cs.execute( """CREATE TABLE %s
                (
                    C1 INTEGER,
                    C2 VARCHAR(3),
                    C3 TIMESTAMP
                )
            """ % self.tableName)

    def _insertData(self, valTuple):
        self.cs.execute( """INSERT INTO %s
                    VALUES (?, ?, ?)
            """ % self.tableName,
            valTuple)

    def _insertDataList(self, valList):
        self.cs.executemany( """INSERT INTO %s
                    VALUES (?, ?, ?)
            """ % self.tableName,
            valList)


    def test_0001_string_parameter_overflow(self):
        """String parameter overflow"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXXX', '2006-08-16 22.33.44.000000')
            )

    def test_0002_refcount_wrong_on_error(self):
        """refcount wrong on cursor error"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXX', '2006-08-16.00.00.00.000000')
            )

    def test_0003_refcount_wrong_on_connection_error(self):
        """refcount wrong on connection error"""
        self.assertRaises(
            DB2.DatabaseError,
            DB2.connect,
            "no_database_here", "sdfds", "sfsdf"
            )
        self.db.close()

    def test_0004_refcount_wrong_on_closed_connection_error(self):
        """refcount wrong on closed connection error"""
        self.db.close()
        self.assertRaises(
            DB2.Error,
            self._createTable
            )
           
if __name__ == '__main__':
    suite = unittest.TestSuite()

    for t in [
            SimpleDB2Test_Crash,
        ]:
        suite.addTest(unittest.makeSuite(t))

    unittest.TextTestRunner(verbosity=2).run(suite)

# FIN
