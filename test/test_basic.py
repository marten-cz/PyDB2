import unittest
import DB2
import Config

class SimpleDB2Test_Connection(unittest.TestCase):
    def setUp(self):
        self.conn_d = Config.ConnDict.copy()

    def test_0000_get_type_num_2_name_dict(self):
        """DB2.SQL_type_dict for description2"""
        d = DB2.SQL_type_dict

    def test_001_ConnectSuccess(self):
        """DB2.connect() - Successful"""
        self.db = DB2.connect(**Config.ConnDict)
        self.db.close()

    def test_002_ConnectFailureWithDB(self):
        """DB2.connect() - Connection failure with wrong database"""
        self.conn_d['dsn'] = 'NonexistentDatabase'
        self.assertRaises( DB2.Error, DB2.connect, **self.conn_d )

    def test_003_ConnectFailureWithUid(self):
        """DB2.connect() - Connection failure with wrong username"""
        self.conn_d['uid'] = 'manyong'
        self.assertRaises( DB2.Error, DB2.connect, **self.conn_d )

    def test_004_ConnectFailureWithPwd(self):
        """DB2.connect() - Connection failure with wrong password"""
        self.conn_d['pwd'] = ''
        self.assertRaises( DB2.Error, DB2.connect, **self.conn_d )

    
class SimpleDB2Test_Cursor(unittest.TestCase):
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
                    C2 VARCHAR(3)
                )
            """ % self.tableName)

    def _insertData(self, valTuple):
        self.cs.execute( """INSERT INTO %s
                    VALUES (?, ?)
            """ % self.tableName,
            valTuple)

    def _insertDataList(self, valList):
        self.cs.executemany( """INSERT INTO %s
                    VALUES (?, ?)
            """ % self.tableName,
            valList)

    ### Acutal test comes here ###

    def test_001_cursor_creation(self):
        """db.cursor() - Successful"""
        pass

    def test_002_execute_create_table(self):
        """cs.execute() - CREATE TABLE"""
        self._createTable()

    def test_0030_execute_insert_values(self):
        """cs.execute() - INSERT (NORMAL)"""
        self._createTable()
        self._insertData( (1, 'a') )

    def test_0031_execute_insert_values(self):
        """cs.execute() - INSERT (NULL)"""
        self._createTable()
        self._insertData( (None, None) )

    def test_0032_execute_insert_values(self):
        """cs.execute() - INSERT (Wrong type)"""
        self._createTable()
        self.assertRaises(
            TypeError,
            self._insertData,
            (1.0, None)
            )

    def test_0033_execute_insert_values(self):
        """cs.execute() - INSERT (Right truncation)"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXXX')
            )

    def test_0034_execute_insert_values(self):
        """cs.execute() - INSERT (Wrong # of params)"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, )
            )

    def test_0040_execute_fetchone(self):
        """cs.fetchone() - SELECT (NORMAL)"""
        self._createTable()
        self._insertData( (1, 'a') )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( tuple(r), (1, 'a') )

    def test_0041_execute_fetchone(self):
        """cs.fetchone() - SELECT (NULL)"""
        self._createTable()
        self._insertData( (None, None) )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( tuple(r), (None, None) )

    def test_0042_execute_fetchone(self):
        """cs.fetchone() - fetch w/o SELECT"""
        self._createTable()
        self._insertData( (None, None) )

        self.assertRaises(DB2.ProgrammingError, self.cs.fetchone)

    def test_0043_execute_fetchone(self):
        """cs.fetchone() - beyond the last result set"""
        self._createTable()
        self._insertData( (None, None) )
        self.cs.execute("""SELECT * FROM %s""" % self.tableName)

        r = self.cs.fetchone()

        for i in range(100):
            r = self.cs.fetchone()
            self.assertEqual(r, None)

    def test_005_executemany(self):
        """cs.executemany()"""
        self._createTable()
        self._insertDataList( [(1, 'a'), (2, 'b'), (None, None)] )

    def test_006_fetchall(self):
        """cs.fetchall()"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        rows = self.cs.fetchall()
        self.assertEqual( len(dataList), len(rows) )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_fetchmany(self):
        """cs.fetchmany()"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 2
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), self.cs.arraysize )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_1_fetchmany_ask_for_too_much(self):
        """cs.fetchmany() - asking for too many rows"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 4
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), len(dataList) )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_1_fetchmany_nothing_available(self):
        """cs.fetchmany() - [ 1411186 ] fix for fetchmany when no rows in rs"""
        self._createTable()
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 2
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), 0)
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )
            
    def test_008_description(self):
        """cs.description & description2"""
        import types

        self._createTable()
        colData = (None, None)
        self._insertData( colData )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        desc = self.cs.description

        # number of columns in result set
        self.assertEqual(len(desc), len(colData))
        for x in desc:
            # each item SHOULD be of Tuple type
            self.assertEqual(type(x), types.TupleType)
            # each item SHOULD be of length 7
            self.assertEqual(len(x), 7)

        desc2 = self.cs.description2
        self.assertEqual(len(desc), len(desc2))
        for x in desc2:
            # each item SHOULD be of Tuple type
            self.assertEqual(type(x), types.TupleType)
            # each item SHOULD be of length 7
            self.assertEqual(len(x), 7)
            # 2nd item SHOULD be of String type
            self.assertEqual(type(x[1]), types.StringType)

    def test_0090_rowcount(self):
        """cs.rowcount - w/ Non-scrollable cursor"""
        self._createTable()
        colData = (None, None)
        self._insertData( colData )
        self.assertEqual(self.cs.rowcount, 1)
        # with non-scrollable cursor (default),
        # it returns -1, not the count of result set
        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        self.assertEqual(self.cs.rowcount, -1)

    def test_0100_callproc(self):
        """cs.callproc() - IN, OUT, INOUT parameters"""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', None, 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0101_callproc(self):
        """cs.callproc() - w/ Result set"""
        self.cs.execute("""
            CREATE TABLE CP_TEST_TB ( P1 INTEGER )
            """)

        SIZE = 100;
        for i in range(SIZE):
            self.cs.execute(
                """INSERT INTO CP_TEST_TB VALUES (?)""", i)

        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 INTEGER)
            LANGUAGE SQL
            BEGIN
                DECLARE CS1 CURSOR WITH RETURN FOR
                    SELECT * FROM CP_TEST_TB;
                OPEN CS1;
            END
            """)

        r = self.cs.callproc("CP_TEST_1", 1)
        self.assertEqual(r, (1, ))
        rows = self.cs.fetchall()
        self.assertEqual(len(rows), SIZE)
        
    def test_0102_callproc(self):
        """cs.callproc() - IN, OUT, INOUT parameters. Out is too short."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 'AA', 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0103_callproc(self):
        """cs.callproc() - IN, OUT, INOUT parameters. Out is too long."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 'AAAAAAA', 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0104_callproc(self):
        """cs.callproc() - IN, OUT, INOUT parameters. Out is wrong type."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 3, 1 )
        self.assertRaises(TypeError, self.cs.callproc, 'CP_TEST_1', params )

class SimpleDB2Test_DictCursor(unittest.TestCase):
    def setUp(self):
        self.db = DB2.connect(**Config.ConnDict)
        self.tableName = 'PYDB2TEST_0'
        self.cs = self.db.cursor(dictCursor=True)

    def tearDown(self):
        self.cs.close()
        self.db.close()

    def _createTable(self):
        self.cs.execute( """CREATE TABLE %s
                (
                    C1 INTEGER,
                    C2 VARCHAR(3)
                )
            """ % self.tableName)

    def _insertData(self, valTuple):
        self.cs.execute( """INSERT INTO %s
                    VALUES (?, ?)
            """ % self.tableName,
            valTuple)

    def _insertDataList(self, valList):
        self.cs.executemany( """INSERT INTO %s
                    VALUES (?, ?)
            """ % self.tableName,
            valList)
            
    def test_001_cursor_creation(self):
        """DictCursor db.cursor() - Successful"""
        pass

    def test_002_execute_create_table(self):
        """DictCursor cs.execute() - CREATE TABLE"""
        self._createTable()

    def test_0030_execute_insert_values(self):
        """DictCursor cs.execute() - INSERT (NORMAL)"""
        self._createTable()
        self._insertData( (1, 'a') )

    def test_0031_execute_insert_values(self):
        """DictCursor cs.execute() - INSERT (NULL)"""
        self._createTable()
        self._insertData( (None, None) )

    def test_0032_execute_insert_values(self):
        """DictCursor cs.execute() - INSERT (Wrong type)"""
        self._createTable()
        self.assertRaises(
            TypeError,
            self._insertData,
            (1.0, None)
            )

    def test_0033_execute_insert_values(self):
        """DictCursor cs.execute() - INSERT (Right truncation)"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, 'XXXX')
            )

    def test_0034_execute_insert_values(self):
        """DictCursor cs.execute() - INSERT (Wrong # of params)"""
        self._createTable()
        self.assertRaises(
            DB2.ProgrammingError,
            self._insertData,
            (1, )
            )

    def test_0040_execute_fetchone(self):
        """DictCursor cs.fetchone() - SELECT (NORMAL)"""
        self._createTable()
        self._insertData( (1, 'a') )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( tuple(r), (1, 'a') )

    def test_0041_execute_fetchone(self):
        """DictCursor cs.fetchone() - SELECT (NULL)"""
        self._createTable()
        self._insertData( (None, None) )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( tuple(r), (None, None) )

    def test_0042_execute_fetchone(self):
        """DictCursor cs.fetchone() - fetch w/o SELECT"""
        self._createTable()
        self._insertData( (None, None) )

        self.assertRaises(DB2.ProgrammingError, self.cs.fetchone)

    def test_0043_execute_fetchone(self):
        """DictCursor cs.fetchone() - beyond the last result set"""
        self._createTable()
        self._insertData( (None, None) )
        self.cs.execute("""SELECT * FROM %s""" % self.tableName)

        r = self.cs.fetchone()

        for i in range(100):
            r = self.cs.fetchone()
            self.assertEqual(r, None)

    def test_005_executemany(self):
        """DictCursor cs.executemany()"""
        self._createTable()
        self._insertDataList( [(1, 'a'), (2, 'b'), (None, None)] )

    def test_006_fetchall(self):
        """DictCursor cs.fetchall()"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        rows = self.cs.fetchall()
        self.assertEqual( len(dataList), len(rows) )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_fetchmany(self):
        """DictCursor cs.fetchmany()"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 2
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), self.cs.arraysize )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_1_fetchmany_ask_for_too_much(self):
        """DictCursor cs.fetchmany() - asking for too many rows"""
        self._createTable()
        dataList = [(1, 'a'), (2, 'b'), (None, None)]
        self._insertDataList( dataList )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 4
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), len(dataList) )
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )

    def test_007_2_fetchmany_nothing_available(self):
        """DictCursor cs.fetchmany() - [ 1411186 ] fix for fetchmany when no rows in rs"""
        self._createTable()
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.arraysize = 2
        rows = self.cs.fetchmany()
        self.assertEqual( len(rows), 0)
        for i in range( len(rows) ):
            self.assertEqual( dataList[i], tuple(rows[i]) )
            
    def test_008_description(self):
        """DictCursor cs.description & description2"""
        import types

        self._createTable()
        colData = (None, None)
        self._insertData( colData )

        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        desc = self.cs.description

        # number of columns in result set
        self.assertEqual(len(desc), len(colData))
        for x in desc:
            # each item SHOULD be of Tuple type
            self.assertEqual(type(x), types.TupleType)
            # each item SHOULD be of length 7
            self.assertEqual(len(x), 7)

        desc2 = self.cs.description2
        self.assertEqual(len(desc), len(desc2))
        for x in desc2:
            # each item SHOULD be of Tuple type
            self.assertEqual(type(x), types.TupleType)
            # each item SHOULD be of length 7
            self.assertEqual(len(x), 7)
            # 2nd item SHOULD be of String type
            self.assertEqual(type(x[1]), types.StringType)

    def test_0090_rowcount(self):
        """DictCursor cs.rowcount - w/ Non-scrollable cursor"""
        self._createTable()
        colData = (None, None)
        self._insertData( colData )
        self.assertEqual(self.cs.rowcount, 1)
        # with non-scrollable cursor (default),
        # it returns -1, not the count of result set
        self.cs.execute("""SELECT * FROM %s""" % self.tableName)
        self.assertEqual(self.cs.rowcount, -1)

    def test_0100_callproc(self):
        """DictCursor cs.callproc() - IN, OUT, INOUT parameters"""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', None, 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0101_callproc(self):
        """DictCursor cs.callproc() - w/ Result set"""
        self.cs.execute("""
            CREATE TABLE CP_TEST_TB ( P1 INTEGER )
            """)

        SIZE = 100;
        for i in range(SIZE):
            self.cs.execute(
                """INSERT INTO CP_TEST_TB VALUES (?)""", i)

        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 INTEGER)
            LANGUAGE SQL
            BEGIN
                DECLARE CS1 CURSOR WITH RETURN FOR
                    SELECT * FROM CP_TEST_TB;
                OPEN CS1;
            END
            """)

        r = self.cs.callproc("CP_TEST_1", 1)
        self.assertEqual(r, (1, ))
        rows = self.cs.fetchall()
        self.assertEqual(len(rows), SIZE)
        
    def test_0102_callproc(self):
        """DictCursor cs.callproc() - IN, OUT, INOUT parameters. Out is too short."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 'AA', 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0103_callproc(self):
        """DictCursor cs.callproc() - IN, OUT, INOUT parameters. Out is too long."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 'AAAAAAA', 1 )
        r = self.cs.callproc('CP_TEST_1', params)
        self.assertNotEqual( params, r )
        self.assertEqual( ('XXXXX', 'YYY', 3), r )

    def test_0104_callproc(self):
        """DictCursor cs.callproc() - IN, OUT, INOUT parameters. Out is wrong type."""
        self.cs.execute(
            """CREATE PROCEDURE CP_TEST_1
            (IN P1 CHAR(5), OUT P2 VARCHAR(5), INOUT P3 INTEGER)
            LANGUAGE SQL
            BEGIN
                SET P2 = 'YYY';
                SET P3 = 3;
            END""")
        params = ( 'XXXXX', 3, 1 )
        self.assertRaises(TypeError, self.cs.callproc, 'CP_TEST_1', params )

class SimpleDB2Test_Extended(unittest.TestCase):
    def setUp(self):
        self.db = DB2.connect(**Config.ConnDict)
        self.tableName = 'PYDB2TEST_1'
        self.cs = self.db.cursor()
        self.type_data = [
                ('CHAR(3)', 'CCC'),
                ('VARCHAR(3)', 'V'),
                ('DATE', '2005-03-03'),
                ('TIME', '00:01:02'),
                #TODO fix timestamp
                ('TIMESTAMP', '2005-03-03-00.01.02.000000'),
                ('SMALLINT', 1),
                ('INTEGER', 2),
                ('BIGINT', 3L),
                ('REAL', 1.0),
                ('FLOAT', 1.0),
                ('DOUBLE', 1.0),
                ('REAL', 100.5),
                ('FLOAT', 100.5),
                ('DECIMAL(6,2)', 100.5),
            ]

    def tearDown(self):
        self.cs.close()
        self.db.close()

    def _pick_col(self, count):
        import random
        cols = []
        for i in range(count):
            cols.append( (i, random.choice(self.type_data)) )

        col_def = ', '.join(['P%d %s' % (x[0], x[1][0]) for x in cols])
        table_def = 'CREATE TABLE %s (%s)' % (self.tableName, col_def)
        def_val = tuple([x[1][1] for x in cols])
        return table_def, def_val

    def __test_randomTable(self, count):
        table_def, def_val = self._pick_col(count)

        self.cs.execute(table_def)
        qmark_list = ', '.join(['?'] * len(def_val))

        self.cs.execute(
            "INSERT INTO %s VALUES (%s)" % (self.tableName, qmark_list),
            def_val
            )

        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.db.rollback()

        return tuple(r) == def_val

    def test_001_randomTable(self):
        """Create table in random manner (columns of random types)"""
        COL_SIZE = 120
        for i in range(100, COL_SIZE):
            r = self.__test_randomTable(i)
            self.assert_(r)

    def __creatSampleTable(self):
        self.cs.execute("""CREATE TABLE %s
            (
                P1 INTEGER,
                P2 VARCHAR(512)
            )
            """ % self.tableName)

    def __test_scrollable_cursor(self, scrollable):
        self.__creatSampleTable()
        data = [ (1, 'a'), (2, 'bb'), (3, 'ccc') ]
        self.cs.executemany(
            "INSERT INTO %s VALUES (?, ?)" % self.tableName,
            data)
        self.cs.set_scrollable(scrollable)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        rows = self.cs.fetchmany( len(data) )
        for i in range( len(data) ):
            self.assertEqual(
                tuple(rows[i]),
                data[i]
            )

    def test_0020_fetchonmany_unscrollable(self):
        """cs.fetchmany() - use Non-Scrollable cursor"""
        self.__test_scrollable_cursor(0)

    def test_0021_fetchonmany_scrollable(self):
        """cs.fetchmany() - use Scrollable cursor"""
        self.__test_scrollable_cursor(1)

    def __creatSampleBLOBTable(self):
        self.cs.execute("""CREATE TABLE %s
            (
                P1 BLOB(1024),
                P2 BLOB(1024)
            )
            """ % self.tableName)

    def test_0030_BLOB(self):
        """BLOB (file)"""
        import os
        self.__creatSampleBLOBTable()
        f = os.tmpfile()
        data = '\xae' * 1024
        f.write(data)
        f.seek(0, 0)
        self.cs.execute(
            "INSERT INTO %s (P1) VALUES (?)" % self.tableName, f
        )
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        rows = self.cs.fetchone()
        self.assertEqual(str(rows[0]), data)

    def __test_BLOB(self, auto_LOB_read):
        SIZE = 10
        self.__creatSampleBLOBTable()
        for i in range(SIZE):
            self.cs.execute(
                "INSERT INTO %s VALUES (?, ?)" % self.tableName,
                (
                    DB2.Binary(chr(i) * i),
                    DB2.Binary(chr(i) * (1024-i))
                )
            )
        self.cs.set_scrollable(1)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.cs.auto_LOB_read = auto_LOB_read
        rows = self.cs.fetchmany(SIZE)
        return rows

    def test_0031_BLOB(self):
        """BLOB (auto_LOB_read ON)"""
        rows = self.__test_BLOB(1)

    def test_0032_BLOB(self):
        """BLOB (auto_LOB_read OFF)"""
        rows = self.__test_BLOB(0)
        for r in rows:
            b = r[0]
            v = self.cs.readLOB(b)

    def test_0040_DATE(self):
        """DATE"""
        import time
        self.cs.execute("""CREATE TABLE %s (P1 DATE) """ % self.tableName)
        ctime = time.time()
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, DB2.DateFromTicks(ctime))
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], str(DB2.DateFromTicks(ctime)) )

    def test_0041_TIME(self):
        """TIME"""
        import time
        self.cs.execute("""CREATE TABLE %s (P1 TIME) """ % self.tableName)
        ctime = time.time()
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, DB2.TimeFromTicks(ctime))
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], str(DB2.TimeFromTicks(ctime)) )

    def test_0042_TIMESTAMP(self):
        """TIMESTAMP"""
        import time
        self.cs.execute("""CREATE TABLE %s (P1 TIMESTAMP) """ % self.tableName)
        ctime = time.time()
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, DB2.TimestampFromTicks(ctime))
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        # XXX
        self.assertEqual( r[0], str(DB2.TimestampFromTicks(ctime)) )
        self.assertEqual( len(r[0]), 26 )

        self.cs.execute("DELETE FROM %s" % self.tableName)

        value = []
        for p in [ '000000', '000010', '000100', '001000', '010000', '100000']:
            ts = '2005-03-09-08.24.59.%s' % p
            value.append(ts)
            self.cs.execute("""INSERT INTO %s
                    VALUES (?)
                """ % self.tableName, ts)

        self.cs.execute("SELECT * FROM %s" % self.tableName)
        rows = self.cs.fetchall()
        for i in range( len(rows) ):
            self.assertEqual(rows[i][0], value[i])

        for i in range(100):
            self.cs.execute("""INSERT INTO %s
                VALUES (CURRENT TIMESTAMP)""" % self.tableName)
            self.cs.execute("SELECT * FROM %s" % self.tableName)
            r = self.cs.fetchone()
            self.assertEqual( len(r[0]), 26 )
            self.cs.execute("DELETE FROM %s" % self.tableName)

    def test_0050_rowcount(self):
        """cs.rowcount - w/ Scrollable cursor"""
        self.__creatSampleTable()
        data = [ (1, 'a'), (2, 'bb'), (3, 'ccc') ]
        self.cs.executemany(
            "INSERT INTO %s VALUES (?, ?)" % self.tableName,
            data)

        self.cs.set_scrollable(1)
        rowCount = self.cs.execute("SELECT * FROM %s" % self.tableName)
        self.assertEqual(rowCount, len(data))
    def test_0060_REAL(self):
        """REAL"""
        self.cs.execute("""CREATE TABLE %s (P1 REAL) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 1.0)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 1.0)

    def test_0061_FLOAT(self):
        """FLOAT"""
        self.cs.execute("""CREATE TABLE %s (P1 FLOAT) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 1.0)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 1.0)

    def test_0062_DOUBLE(self):
        """DOUBLE"""
        self.cs.execute("""CREATE TABLE %s (P1 DOUBLE) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 1.0)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 1.0)

    def test_0072_DECIMAL_from_int(self):
        """DECIMAL from int"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 1)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 1.0)

    def test_0073_DECIMAL_from_long(self):
        """DECIMAL from long"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 1L)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 1.0)
        
    def test_0074_DECIMAL_from_none(self):
        """DECIMAL from none"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, None)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], None)
        
    def test_0075_DECIMAL_from_float(self):
        """DECIMAL from float"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 10000.50)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        #truncated due to missing decimals
        #TODO shouldn't the truncation issue a warning?
        self.assertEqual( r[0], 10000.0)

    def test_0076_DECIMAL_from_float(self):
        """DECIMAL from float, float too many decimals"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL(9,2)) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 10000.56234)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        #truncated due to missing decimals
        #TODO shouldn't the truncation issue a warning?
        self.assertEqual( r[0], 10000.56)

    def test_0077_DECIMAL_from_float_too_big(self):
        """DECIMAL from float, float too big to fit"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL(3,2)) """ % self.tableName)
        #TODO shouldn't this be a ValueError?
        self.assertRaises(DB2.Error, self.cs.execute, """INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 10000.50)
        
    def test_0078_DECIMAL_from_float(self):
        """DECIMAL from float, negative"""
        self.cs.execute("""CREATE TABLE %s (P1 DECIMAL(9,2)) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, -10000.56234)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        #truncated due to missing decimals
        #TODO shouldn't the truncation issue a warning?
        self.assertEqual( r[0], -10000.56)
        
    def test_0081_NUMERIC_from_float(self):
        """NUMERIC from float"""
        self.cs.execute("""CREATE TABLE %s (P1 NUMERIC) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 100.5)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        #truncation
        self.assertEqual( r[0], 100.0)

    def test_0082_NUMERIC_from_int(self):
        """NUMERIC from int"""
        self.cs.execute("""CREATE TABLE %s (P1 NUMERIC) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 100)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 100.0)

    def test_0083_NUMERIC_from_long(self):
        """NUMERIC from long"""
        self.cs.execute("""CREATE TABLE %s (P1 NUMERIC) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, 100L)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], 100.0)
    def test_0084_NUMERIC_from_none(self):
        """NUMERIC from None"""
        self.cs.execute("""CREATE TABLE %s (P1 NUMERIC) """ % self.tableName)
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, None)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( r[0], None)
        
    def test_0091_LONG_VARBINARY(self):
        """Long varbinary"""
        self.cs.execute("""CREATE TABLE %s (P1 LONG VARCHAR FOR BIT DATA) """ % self.tableName)
        bits = "AA"*32700
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, bits)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( len(r[0]), len(bits))
        self.assertEqual( r[0], bits)
        
    def test_0092_VARBINARY(self):
        """Varbinary"""
        self.cs.execute("""CREATE TABLE %s (P1 VARCHAR(20) FOR BIT DATA) """ % self.tableName)
        bits = "AA"*20
        self.cs.execute("""INSERT INTO %s
                VALUES (?)
            """ % self.tableName, bits)
        self.cs.execute("SELECT * FROM %s" % self.tableName)
        r = self.cs.fetchone()
        self.assertEqual( len(r[0]), len(bits))
        self.assertEqual( r[0], bits)
        
    def test_0093_VARBINARY_OVERFLOW(self):
        """Varbinary overflow"""
        self.cs.execute("""CREATE TABLE %s (P1 VARCHAR(20) FOR BIT DATA) """ % self.tableName)
        bits = "AA"*21
        self.assertRaises(DB2.ProgrammingError, self.cs.execute, """INSERT INTO %s
                    VALUES (?)
                    """ % self.tableName, bits)
        
        
class SimpleDB2Test_Regression(unittest.TestCase):
    def setUp(self):
        self.db = DB2.connect(**Config.ConnDict)
        cursor = self.db.cursor()
        cursor.execute("CREATE TABLE test1(A BIGINT, B VARCHAR(50), C BIGINT)")
        cursor.close()

    def tearDown(self):
        cursor = self.db.cursor()
        cursor.execute("drop table test1")
        self.db.close()

    def test_select_from_insert(self):
        """Patch[ 958351 ] Fix Select DML Syntax segmentation fault"""
        cursor = self.db.cursor()
        insert_row = (1,"bla", 3)
        rowCount = cursor.execute("""
            SELECT *
            FROM FINAL TABLE
            (
                INSERT into test1(a,b,c) VALUES (?,?,?)
            )
            """, insert_row)
        self.assertEqual(rowCount, 1)
        ##this used to segfault AFAIK
        row = cursor.fetchone()
        self.assertEqual(row, insert_row)

if __name__ == '__main__':
    suite = unittest.TestSuite()

    for t in [
            SimpleDB2Test_Connection,
            SimpleDB2Test_Cursor,
            SimpleDB2Test_DictCursor,
            SimpleDB2Test_Extended,
            SimpleDB2Test_Regression,
        ]:
        suite.addTest(unittest.makeSuite(t))

    unittest.TextTestRunner(verbosity=2).run(suite)

# FIN
