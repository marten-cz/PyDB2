"""

	DB API v2.0 Module for IBM DB2

	v1.1.x

	Man-Yong Lee <manyong.lee@gmail.com>, 2005

	2005-11-23 Frank Balzer <frank.balzer@novell.com>
	   - add multiple result set support for .executemany
		 - add multiple result set support for callproc
		 - add .nextset support
		 - fixed fetchXXX to return a list
		 

"""
import _db2
import os
import sys
import time
import types
import binascii
import string

__all__ = [
	'Binary', 'BLOB',
	'Date', 'Time', 'Timestamp',
	'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
	'Error',
	'connect', 'Connection', 'connection',
	]

# /* Exceptions */ #
Warning			= _db2.Warning
Error			= _db2.Error
InterfaceError		= _db2.InterfaceError
DatabaseError		= _db2.DatabaseError
DataError		= _db2.DataError
OperationalError	= _db2.OperationalError
IntegrityError		= _db2.IntegrityError
InternalError		= _db2.InternalError
ProgrammingError	= _db2.ProgrammingError
NotSupportedError	= _db2.NotSupportedError

class BaseType_:
	def __cmp__(self, other):
		return cmp(self.value, other.value)

	def __str__(self):
		return self.get_SQL_value()

class LOBLoc(BaseType_):
	def __init__(self, loc, type):
		self.value = (loc, type)

class Binary(BaseType_):
	def __init__(self, s):
		self.value = s

	def __repr__(self):
		return '<Binary len: %d>' % len(self.value)

	def __str__(self):
		return self.value

	def get_SQL_value(self):
		return binascii.b2a_hex(self.value)

BLOB = Binary

class Date(BaseType_):
	def __init__(self, year, month, day):
		self.value = ('date', year, month, day)

	def get_SQL_value(self):
		return '%04d-%02d-%02d' % self.value[1:]

class Time(BaseType_):
	def __init__(self, hour, min, sec):
		self.value = ('time', hour, min, sec)

	def get_SQL_value(self):
		return '%02d:%02d:%02d' % self.value[1:]

class Timestamp(BaseType_):
	def __init__(self, year, month, day, hour, min, sec):
		self.value = ('timestamp', year, month, day, hour, min, sec)

	def get_SQL_value(self):
		#
		#return '%04d-%02d-%02d-%02d.%02d.%02d.000000' % self.value[1:]
		#return an ISO Date
		return '%04d-%02d-%02d %02d:%02d:%02d.000000' % self.value[1:]

def DateFromTicks(ticks=None):
	if ticks == None: ticks = time.time()
	return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks=None):
	if ticks == None: ticks = time.time()
	return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks=None):
	if ticks == None: ticks = time.time()
	return Timestamp(*time.localtime(ticks)[:6])

FETCH_NEXT	= 1
FETCH_FIRST	= 2
FETCH_LAST	= 3
FETCH_PRIOR	= 4
FETCH_ABSOLUTE	= 5
FETCH_RELATIVE	= 6

SQL_type_dict = _db2.get_SQL_type_dict()

class Cursor:
	def __init__(self, _cs):
		self._cs = _cs
		self.arraysize = 10
		self.auto_LOB_read = 1
		self.resultsets = []
		self.haveresultsets = 0
		

	def __del__(self):
		pass

	def _description2(self):
		''' description: 
			(Name, TypeCode,
			DisplaySize, InternalSize,
			Precision, Scale,
			NullOk)
		'''
		desc = self._cs.description

		if desc == None:
			return None

		r = []
		for t in desc:
			t = list(t)
			t[1] = SQL_type_dict.get(t[1], "?(%d)" % t[1])
			r.append( tuple(t) )
		return tuple(r)

	def __getattr__(self, name):
		if name == "description2":
			return self._description2()

		return getattr(self._cs, name)

	def _convert_params(self, args):
		args = list(args)
		for i in range( len(args) ):
			if isinstance(args[i], BaseType_):
				args[i] = args[i].get_SQL_value()

		return tuple(args)

	def _sql_execute(self, func, what, *args):
        	# Parameters may be either a list or tuple of values (sequence)
		if len(args) == 1 and type(args[0]) in (tuple, list):
			# ( (1, ...), ) --> (1, ...)
			args = args[0]

		args = self._convert_params(args)
		return func(what, *(args, ))

	def execute(self, stmt, *args):
		self.haveresultsets = 0
		return self._sql_execute(self._cs.execute, stmt, *args)

	def executemany(self, stmt, seq_params):
		self.resultsets = []
		self.haveresultsets = 0
		for p in seq_params:
			self.execute(stmt, *(p, ))
			s = string.lower(stmt)
			if ( s.find( "select" ) != -1 ):
				aa = self.fetchall()
				if aa:
					self.resultsets.append(aa)
		if len(self.resultsets):
			self.haveresultsets = 1

	def callproc(self, procname, *args):
		self.haveresultsets = 0
		return self._sql_execute(self._cs.callproc, procname, *args)

	def readLOB(self, LOB):
		return self._cs._readLOB(*LOB.value)

	def _convert_result_col(self, ret):
		type = ret[0]
		if type in ( "blob", "clob" ):
			loc = ret[1]
			if self.auto_LOB_read:
				r = self.readLOB( LOBLoc(loc, type) )
				if type == "blob":
					return Binary(r)
				else:
					return r
			else:
				return LOBLoc(loc, type)
		else:
			return ret

	def _convert_result_rows(self, rows):
		TupleType = types.TupleType

		for r in rows:
			for i in range(len(r)):
				if type(r[i]) == TupleType:
					r[i] = self._convert_result_col(r[i])
		return rows

	def fetchone(self, **kwargs):
		if self.haveresultsets and len(self.resultsets[0]):
			a = self.resultsets[0][0] 
			del self.resultsets[0][0]
			return a
		r = self._cs.fetch(-1)
		if not r: return r

		TupleType = types.TupleType
		for i in range(len(r)):
			if type(r[i]) == TupleType:
				r[i] = self._convert_result_col(r[i])
		# Returning a tuple for backwards compatibility
		#return tuple(r)
		# return a list as it should be
		return r
	
	def fetchmany(self, size=None):
		if self.haveresultsets and len(self.resultsets[0]):
			if size == None: size = self.arraysize
			a = []
			while size:
				a.append( self.resultsets[0][0] )
				del self.resultsets[0][0]
				size = size - 1
				if not len( self.resultsets[0] ):
					return a
			return a
				
				
		if size == None: size = self.arraysize
		if size <= 0:
			return []

		rlist = self._cs.fetch(size)
		if not rlist:
			return []
		return self._convert_result_rows(rlist)

	def fetchall(self):
		if self.haveresultsets and len(self.resultsets):
			return self.resultsets[0]
		rlist = []
		while 1:
			r = self.fetchone()
			if not r:
				break
			rlist.append(r)
		if not len(rlist):
			return []
		return rlist

	def nextset(self):
		if len(self.resultsets) and self.haveresultsets:
			del self.resultsets[0]
			if len(self.resultsets):
				return 1
			else:
				self.haveresultsets = 0
				return 0
			
		return self._cs.nextset()

	def setinputsizes(self, sizes):
		pass

	def setoutputsize(self, size, column=-1):
		pass

	def get_timeout(self):
		return self._cs._timeout()

	def set_timeout(self, timeout):
		return self._cs._timeout(timeout)

	def get_scrollable(self):
		return self._cs._scrollable()

	def set_scrollable(self, scrollable):
		return self._cs._scrollable(scrollable)

	def _skip(self, howmany=1):
		return self._cs._skip(howmany)

	def scroll(self, value, mode='relative'):
		if value == 0:
			return 0

		if mode == 'absolute':
			raise NotSupportedError
		if value < 0:
			raise ValueError, "value SHOULD be >= 1"
		return self._skip(value)

class DictCursor(Cursor):
	def fetchone(self):
		r = {}
		data = Cursor.fetchone(self)

		desc = self._cs.description

		for i in range(len(desc)):
			name = desc[i][0]
			r[name] = data[i]

		return r

class Connection:
	def __init__(self, *args, **kwargs):
		self._db = _db2.connect(*args, **kwargs)

	def __del__(self):
		pass

	def __str__(self):
		return str(self._db)

	def close(self):
		self._db.close()

	def cursor(self, dictCursor=0):
		if dictCursor:
			cursorClass = DictCursor
		else:
			cursorClass = Cursor
		return cursorClass(self._db.cursor())

	def commit(self):
		self._db.commit()

	def rollback(self):
		self._db.rollback()

connect = Connection

apilevel	= '2.0'

# 0 Threads may not share the module.
# 1 Threads may share the module, but not connections.
# 2 Threads may share the module and connections.
# 3 Threads may share the module, connections and cursors.

threadsafety	= 2
paramstyle	= 'qmark'

# FIN
