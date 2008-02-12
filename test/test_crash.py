import unittest
import DB2
import Config

import sys
wait_for_keypress = 1
if not "python_d" in sys.executable:
    sys.exit("ERROR: Must run in debug mode (build with debug flag and run with python_d.exe)")
    
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
                    C3 TIME,
                )
            """ % self.tableName)

    def _insertData(self, valTuple):
        self.cs.execute( """INSERT INTO %s
                    VALUES (?, ?, ?)
            """ % self.tableName,
            valTuple)

    def _insertDataList(self, valList):
        self.cs.executemany( """INSERT INTO %s
                    VALUES (?, ?)
            """ % self.tableName,
            valList)


    def test_0001_string_parameter_overflow(self):
        """String parameter overflow"""
        self._createTable()
        if wait_for_keypress:
            raw_input("Press Enter when you've set up debugging")
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXXX', '23:45:56')
            )

    def test_0002_wrong_dateformat(self):
        """String parameter overflow"""
        self._createTable()
        if wait_for_keypress:
            raw_input("Press Enter when you've set up debugging")
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXX', '23.45.56')
            )

if __name__ == '__main__':
    suite = unittest.TestSuite()

    for t in [
            SimpleDB2Test_Crash,
        ]:
        suite.addTest(unittest.makeSuite(t))

    unittest.TextTestRunner(verbosity=2).run(suite)

# FIN
