/*
	Python DB API for IBM DB2

	Man-Yong Lee <manyong.lee@gmail.com>, 2000
	             <yong@linuxkorea.co.kr>
         Jon Thoroddsen    <jon.thoroddsen@gmail.com>     
 	2005-11-23 Frank Balzer <frank.balzer@novell.com> (Audited by Jon Thoroddsen at 2008-02-13)
		-- fixed some compiler warnings
		-- fix for a bufferoverflow in _DB2CursorObj_prepare_param_vars
	According to PEP 249 (Python DB API Spec v2.0)

 */
#define __version__	"1.1.0"

#include "Python.h"

#ifdef MS_WIN32
#	include <windows.h>
#	ifndef unit
#		define unit unsigned int
#	endif
#endif /* MS_WIN32 */ 

#include "structmember.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sqlcli1.h>
#include <sqlenv.h>

#ifndef TRUE 
#define TRUE	1
#define FALSE	0
#endif

#define	DEBUG_MSG(msg)				\
	if (DEBUG) {				\
		fprintf(stderr, msg "\n");	\
	}

/* developer's debug mode */
static int DEBUG = 0;

static int ignoreWarning = 1;	/* SQL_SUCCESS_WITH_INFO as SQL_SUCCESS */

/* MY_MALLOC */
#define MY_MALLOC               PyMem_Malloc
#define MY_FREE                 PyMem_Free

typedef struct {
	SQLSMALLINT	typeNum;
	char		*typeName;
} DB2SQLTypeNameStruct;

static DB2SQLTypeNameStruct DB2SQLTypeNameMap[] = {
	{ SQL_BIGINT,		"BIGINT" },
	{ SQL_BINARY,		"BINARY" },
	{ SQL_BLOB,		"BLOB" },
	{ SQL_BLOB_LOCATOR,	"BLOB_LOCATOR" },
	{ SQL_CHAR,		"CHAR" },
	{ SQL_CLOB,		"CLOB" },
	{ SQL_CLOB_LOCATOR,	"CLOB_LOCATOR" },
	{ SQL_TYPE_DATE,	"DATE" },
	{ SQL_DBCLOB,		"DBCLOB" },
	{ SQL_DBCLOB_LOCATOR,	"DBCLOB_LOCATOR" },
	{ SQL_DECIMAL,		"DECIMAL" },
	{ SQL_DOUBLE,		"DOUBLE" },
	{ SQL_FLOAT,		"FLOAT" },
	{ SQL_GRAPHIC,		"GRAPHIC" },
	{ SQL_INTEGER,		"INTEGER" },
	{ SQL_LONGVARCHAR,	"LONGVARCHAR" },
	{ SQL_LONGVARBINARY,	"LONGVARBINARY" },
	{ SQL_LONGVARGRAPHIC,	"LONGVARGRAPHIC" },
	{ SQL_NUMERIC,		"NUMERIC" },
	{ SQL_REAL,		"REAL" },
	{ SQL_SMALLINT,		"SMALLINT" },
	{ SQL_TYPE_TIME,	"TIME" },
	{ SQL_TYPE_TIMESTAMP,	"TIMESTAMP" },
	{ SQL_VARCHAR,		"VARCHAR" },
	{ SQL_VARBINARY,	"VARBINARY" },
	{ SQL_VARGRAPHIC,	"VARGRAPHIC" },
	{ SQL_DATALINK,		"DATALINK" },
	{ 0, "" },	/* sentinel */
};

char *get_SQL_type_name(SQLSMALLINT);

typedef struct {
	int		rc;
	char		*rcName;
} DB2SQLRcNameStruct;

static DB2SQLRcNameStruct DB2SQLRcNameMap[] = {
	{ SQL_SUCCESS,			"SQL_SUCCESS" },
	{ SQL_SUCCESS_WITH_INFO,	"SQL_SUCCESS_WITH_INFO" },
	{ SQL_ERROR,			"SQL_ERROR" },
	{ SQL_INVALID_HANDLE,		"SQL_INVALID_HANDLE" },
	{ SQL_NO_DATA_FOUND,		"SQL_NO_DATA_FOUND" },
	{ SQL_STILL_EXECUTING,		"SQL_STILL_EXECUTING" },
	{ SQL_NEED_DATA,		"SQL_NEED_DATA" },
	{ 0, "" },	/* sentinel */
};

void show_rc_name(char *, SQLRETURN);

/* ###########################################################################

   Type Object

   ######################################################################### */

static PyObject		*DB2_Error;
static PyObject		*DB2_Warning;
static PyObject		*DB2_InterfaceError;
static PyObject		*DB2_DatabaseError;
static PyObject		*DB2_InternalError;
static PyObject		*DB2_OperationalError;
static PyObject		*DB2_ProgrammingError;
static PyObject		*DB2_IntegrityError;
static PyObject		*DB2_DataError;
static PyObject		*DB2_NotSupportedError;

const char *sqlState_4_DataError[] = {
	"07006",	/* Invalid conversion */
	NULL,
};

const char *sqlSate_4_OperationalError[] = {
	"25000",	/* Invalid transcation state */
	"25501",
	"40003",	/* Communication link failure */
	"08S01",
	"40001",	/* rolled back because of a deadlock or timeout */
	"57011",	/* The transaction log for the database is full */
	NULL,
};

const char *sqlState_4_ProgrammingError[] = {
	"HY010",	/* Function sequence error */
	"07001",	/* Wrong number of parameters */
	"22007",	/* Invalid datetime format */
	"22001",	/* String data right truncation */
	"24000",	/* Invalid cursor state */
	"01504",	/* The SQL statement will modify an entire table or view. */
	NULL,
};

#define MAX_UID_LENGTH	18
#define MAX_PWD_LENGTH	30
#define MAX_STR_LEN	255

typedef struct {
	PyObject_HEAD
	SQLHANDLE	henv;		/* environment handle */
	SQLHANDLE	hdbc;		/* connection handle */
	SQLRETURN	sqlrc;

	char		dbAlias[SQL_MAX_DSN_LENGTH + 1];
	char		uid[MAX_UID_LENGTH + 1];
	char		pwd[MAX_PWD_LENGTH + 1];

	int		connected;
	int		autoCommit;

	/* misc. */
	char		serverName[MAX_STR_LEN];
	char		dbmsName[MAX_STR_LEN];
	char		dbmsVer[MAX_STR_LEN];
	char		driverName[MAX_STR_LEN];
	char		driverVer[MAX_STR_LEN];

} DB2ConnObj;

#define	CHECK_CONN_RC(rc)			\
	if (!checkSuccess(rc)) {		\
		return _DB2ConnObj_Conn_Error(self, NULL);	\
	}

staticforward PyTypeObject	DB2ConnObj_Type;

static PyObject * _DB2_GetDiagRec(SQLHANDLE, SQLSMALLINT);
static int checkSuccess(SQLRETURN);
static int checkWarning(SQLRETURN);
static int includeSqlState(char *, const char *[]);
static PyObject * determineException(char *);

static PyObject * _db2_connect(PyObject *, PyObject *, PyObject *);
static PyObject * _db2_SQL_type_dict(PyObject *, PyObject *);

static void DB2ConnObj_dealloc(DB2ConnObj *);
static PyObject * DB2ConnObj_getattr(DB2ConnObj *, char *);
static PyObject * DB2ConnObj_close(DB2ConnObj *, PyObject *);
static PyObject * DB2ConnObj_end_tran(DB2ConnObj *, int);
static PyObject * DB2ConnObj_commit(DB2ConnObj *, PyObject *);
static PyObject * DB2ConnObj_rollback(DB2ConnObj *, PyObject *);
static PyObject * DB2ConnObj_cursor(DB2ConnObj *, PyObject *);
static PyObject * DB2ConnObj_repr(DB2ConnObj *);
static PyObject * _DB2ConnObj_Conn_Error(DB2ConnObj *, PyObject *);
static PyObject * _DB2ConnObj_Disconnected_Error(DB2ConnObj *);

static void DB2ConnObj_set_conn_info(DB2ConnObj *);

static PyMethodDef
DB2ConnObj_methods[] = {
	{ "close", (PyCFunction)DB2ConnObj_close, METH_VARARGS, },
	{ "commit", (PyCFunction)DB2ConnObj_commit, METH_VARARGS, },
	{ "rollback", (PyCFunction)DB2ConnObj_rollback, METH_VARARGS, },
	{ "cursor", (PyCFunction)DB2ConnObj_cursor, METH_VARARGS, },
	{ NULL, NULL }
};

typedef struct {

	int		type;
	int		typeEx;
	SQLPOINTER	buf;
	SQLINTEGER	bufLen;
	SQLINTEGER	*outLen;

} DB2BindStruct;

typedef struct {

	void		*buf;
	SQLINTEGER	bufLen;

	SQLSMALLINT	inOutType;	/* callproc */

	SQLSMALLINT	dataType;
	SQLUINTEGER	colSize;
	SQLSMALLINT	decDigits;
	SQLSMALLINT	nullable;

	SQLINTEGER	outLen;

} DB2ParamStruct;

typedef struct {
	PyObject_HEAD

	SQLHANDLE	hdbc;		/* Connection handle from ConnObj */
	SQLHANDLE	hstmt;		/* Statement handle */
	int		opened;

	SQLCHAR		*lastStmt;

	PyObject	*description;

	DB2BindStruct	**bindColList;
	int		bindColCount;

	DB2ParamStruct	**paramList;
	int		paramCount;

	int		bScrollable;

	int		lastArraySize;
	int		rowCount;
	int		bFetchRefresh;
	SQLUINTEGER	nFetchedRows;
	SQLUSMALLINT	*rowStatus;

	PyObject	*messages;	/* Optional DB API Extensions */

	int		timeout;
} DB2CursorObj;

staticforward PyTypeObject	DB2CursorObj_Type;

static struct memberlist DB2CursorObj_members[] = {
	{ "description", T_OBJECT, offsetof(DB2CursorObj, description), 0 },
	{ "rowcount", T_INT, offsetof(DB2CursorObj, rowCount), 0 },
	{ "messages", T_OBJECT, offsetof(DB2CursorObj, messages), 0 },
	{ NULL }
};

#define	CHECK_CURSOR_RC(rc)				\
	if (!checkSuccess(rc)) {			\
		return _DB2CursorObj_Cursor_Error(self, NULL);	\
	}

static void DB2CursorObj_dealloc(DB2CursorObj *);
static PyObject * DB2CursorObj_getattr(DB2CursorObj *, char *);
static PyObject * DB2CursorObj_close(DB2CursorObj *, PyObject *);
static PyObject * DB2CursorObj_execute(DB2CursorObj *, PyObject *);
static PyObject * DB2CursorObj_fetch(DB2CursorObj *, PyObject *);
static PyObject * DB2CursorObj_callproc(DB2CursorObj *, PyObject *);
static PyObject * DB2CursorObj_skip_rows(DB2CursorObj *, PyObject *);

static int _DB2CursorObj_set_col_desc(DB2CursorObj *, int);
static int _DB2CursorObj_get_param_info(DB2CursorObj *, int);
static int _DB2CursorObj_prepare_param_vars(DB2CursorObj *, int, PyObject *);
static int _DB2CursorObj_bind_col(DB2CursorObj *, int, int);
static int _DB2CursorObj_reset_bind_col(DB2CursorObj *, int);
static int _DB2CursorObj_reset_params(DB2CursorObj *, int);
static int _DB2CursorObj_reset_cursor(DB2CursorObj *);
static PyObject * _SQL_CType_2_PyType(DB2BindStruct *, int);
static PyObject * _SQLType_2_PyType(DB2ParamStruct *);
static PyObject * _DB2CursorObj_retrieve_rows(DB2CursorObj *, int);
static PyObject * DB2CursorObj_read_LOB(DB2CursorObj *, PyObject *);
static int _DB2CursorObj_send_lob_file(DB2CursorObj *, FILE *);
static PyObject * _DB2CursorObj_get_Cursor_Error(DB2CursorObj *, PyObject *);
static PyObject * _DB2CursorObj_Cursor_Error(DB2CursorObj *, PyObject *);
static PyObject * _DB2CursorObj_fill_Cursor_messages(DB2CursorObj *);
static PyObject * DB2CursorObj_timeout(DB2CursorObj *, PyObject *);
static PyObject * DB2CursorObj_scrollable_flag(DB2CursorObj *, PyObject *);
void _DB2CursorObj_timeout(DB2CursorObj *, int);

static void set_param_type_error(int, SQLSMALLINT, char *);

static PyMethodDef
DB2CursorObj_methods[] = {
	{ "close", (PyCFunction)DB2CursorObj_close, METH_VARARGS, },
	{ "execute", (PyCFunction)DB2CursorObj_execute, METH_VARARGS, },
	{ "fetch", (PyCFunction)DB2CursorObj_fetch, METH_VARARGS, },
	{ "callproc", (PyCFunction)DB2CursorObj_callproc, METH_VARARGS, },
	{ "_skip", (PyCFunction)DB2CursorObj_skip_rows, METH_VARARGS, },
	{ "_readLOB", (PyCFunction)DB2CursorObj_read_LOB, METH_VARARGS, },
	{ "_timeout", (PyCFunction)DB2CursorObj_timeout, METH_VARARGS, },
	{ "_scrollable", (PyCFunction)DB2CursorObj_scrollable_flag, METH_VARARGS, },
	{ NULL, NULL }
};

/* ##########################################################################

   METHOD

   ######################################################################### */

static int
checkSuccess(SQLRETURN  rc)
{
	if ( rc == SQL_SUCCESS ) {
		return 1;
	} else if ( rc == SQL_SUCCESS_WITH_INFO ) {
		if (ignoreWarning) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 0;
	}
}

static int
checkWarning(SQLRETURN  rc)
{
	if ( rc == SQL_SUCCESS_WITH_INFO ) {
		return 1;
	} else {
		return 0;
	}
}

static int
includeSqlState(char *sqlState, const char *stateList[])
{
	int i;
	int ret = 0;
	const char *v;

	i = 0;
	while (1) {
		v = stateList[i];
		if (v == NULL) { break; }
		if (strcmp(sqlState, v) == 0) {
			ret = 1;
		}
		i += 1;
	}

	return ret;
}

static PyObject *
determineException(char *sqlState)
{
	PyObject *exc;

	if (DEBUG) { fprintf(stderr, "* sqlState: <%s>\n", sqlState); }

	if (includeSqlState(sqlState, sqlState_4_DataError)) {
		exc = DB2_DataError;
	} else if (includeSqlState(sqlState, sqlSate_4_OperationalError)) {
		exc = DB2_OperationalError;
	} else if (includeSqlState(sqlState, sqlState_4_ProgrammingError)) {
		exc = DB2_ProgrammingError;
	} else {
		exc = DB2_Error;
	}
	return exc;
}

static PyObject *
_DB2CursorObj_get_Cursor_Error(DB2CursorObj *self, PyObject *exc)
{
	PyObject *t, *r;
	char *v;

	t = _DB2_GetDiagRec(self->hstmt, SQL_HANDLE_STMT);
	v = PyString_AsString(PyTuple_GetItem(t, 0));

	if (!exc) { exc = determineException(v); }

	r = PyTuple_New(2);

	PyTuple_SetItem(r, 0, exc);
	PyTuple_SetItem(r, 1, t);

	return r;
}

static PyObject *
_DB2CursorObj_Cursor_Error(DB2CursorObj *self, PyObject *exc)
{
	PyObject *e, *v, *r;

	r = _DB2CursorObj_get_Cursor_Error(self, exc);

	e = PyTuple_GetItem(r, 0); Py_INCREF(e);
	v = PyTuple_GetItem(r, 1); Py_INCREF(v);

	PyErr_SetObject(e, v);

	Py_DECREF(r);

	return NULL;
}

static PyObject *
_DB2CursorObj_fill_Cursor_messages(DB2CursorObj *self)
{
	PyObject *t, *r;

	t = _DB2_GetDiagRec(self->hstmt, SQL_HANDLE_STMT);

	r = PyTuple_New(2);

	Py_INCREF(DB2_Warning);
	PyTuple_SetItem(r, 0, DB2_Warning);
	PyTuple_SetItem(r, 1, t);

	PyList_Append( self->messages, r);

	Py_DECREF(r);

	return NULL;
}

static PyObject *
_DB2ConnObj_Conn_Error(DB2ConnObj *self, PyObject *exc)
{
	PyObject *t;
	char *v;

	t = _DB2_GetDiagRec(self->hdbc, SQL_HANDLE_DBC);
	v = PyString_AsString(PyTuple_GetItem(t, 0));

	if (!exc) { exc = determineException(v); }

	PyErr_SetObject(exc, t);
	return NULL;
}

static PyObject *
_DB2ConnObj_Disconnected_Error(DB2ConnObj *self)
{
	PyObject	*t;

	t = PyTuple_New(3);

	PyTuple_SetItem(t, 0, PyString_FromString(""));
	PyTuple_SetItem(t, 1, PyInt_FromLong((long)-1));
	PyTuple_SetItem(t, 2, PyString_FromString("Disconnected"));

	PyErr_SetObject(DB2_Error, t);
	return NULL;
}

static PyObject *
_DB2_GetDiagRec(SQLHANDLE handle, SQLSMALLINT handleType)
{
	SQLCHAR		sqlState[ SQL_SQLSTATE_SIZE + 1 ];
	/*
		CCSSS
		C: Class
		S: Subclass

		'01'	: a warning
		'HY'	: generated by DB2 CLI or ODBC driver
		'IM'	: generated by ODBC driver
	*/
	SQLCHAR		msgText[ SQL_MAX_MESSAGE_LENGTH + 1 ];
	SQLSMALLINT	msgTextOutLen = 0;
	SQLINTEGER	nativeErr;
	SQLRETURN	rc;

	PyObject	*t;

	rc = SQLGetDiagRec(
		handleType,
		handle,
		(SQLSMALLINT)1,		/* RecNumber */
		sqlState,
		&nativeErr,
		msgText,
		sizeof(msgText),
		&msgTextOutLen);

	t = PyTuple_New(3);

	if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
		PyTuple_SetItem(t, 0,
			PyString_FromStringAndSize(sqlState, 5));
		PyTuple_SetItem(t, 1,
			PyInt_FromLong((long)nativeErr));
		PyTuple_SetItem(t, 2,
			PyString_FromStringAndSize(msgText, msgTextOutLen));
	} else if (rc == SQL_INVALID_HANDLE) {
		PyTuple_SetItem(t, 0, PyString_FromString("I"));
		PyTuple_SetItem(t, 1, PyInt_FromLong((long)-1));
		PyTuple_SetItem(t, 2, PyString_FromString("Invalid Handle"));
	} else if (rc == SQL_ERROR) {
		PyTuple_SetItem(t, 0, PyString_FromString("E"));
		PyTuple_SetItem(t, 1, PyInt_FromLong((long)-1));
		PyTuple_SetItem(t, 2, PyString_FromString("Error"));
	} else {
		PyTuple_SetItem(t, 0, PyString_FromString("?"));
		PyTuple_SetItem(t, 1, PyInt_FromLong((long)-1));
		PyTuple_SetItem(t, 2,
			PyString_FromString("SQLGetDiagRec() failed"));
	}

	return t;
}

char *
get_SQL_type_name(SQLSMALLINT dataType)
{
	size_t i;
	for (i=0; i < sizeof(DB2SQLTypeNameMap); i++) {
		if (DB2SQLTypeNameMap[i].typeNum == dataType) {
			return DB2SQLTypeNameMap[i].typeName;
		}
	}
	return "";
}

void
show_rc_name(char *prefix, SQLRETURN rc)
{
	size_t i;

	if (!DEBUG) {
		return;
	}

	for (i=0; i < sizeof(DB2SQLRcNameMap); i++) {
		if (DB2SQLRcNameMap[i].rc == rc) {
			fprintf(stderr, "* %s : %s\n",
				prefix,
				DB2SQLRcNameMap[i].rcName);
			return;
		}
	}
	fprintf(stderr, "* %s : %d\n", prefix, (int)rc);
}

/* #### MODULE METHODS ### */

static PyObject *
_db2_connect(PyObject *self, PyObject *args, PyObject *kwargs)
{
	static char *kwList[] = {
			"dsn", "uid", "pwd",
			"autoCommit", "connectType", NULL,
	};
	char *dsn	= "sample";
	char *uid	= "db2inst1";
	char *pwd	= "ibmdb2";
	int autoCommit	= 0;	/* no auto-commit mode */
	int connectType	= 1;	/* single database per Unit of Work */

	SQLRETURN	rc;

	DB2ConnObj	*c = NULL;

	if (!PyArg_ParseTupleAndKeywords(
		args, kwargs,
		"|sssii", kwList,
		&dsn, &uid, &pwd,
		&autoCommit, &connectType)) {
		return NULL;
	}

	if (!(c = PyObject_New(DB2ConnObj, &DB2ConnObj_Type))) {
		return PyErr_NoMemory();
	}

	c->connected		= FALSE;
	c->serverName[0]	= '\0';
	c->dbmsName[0]		= '\0';
	c->dbmsVer[0]		= '\0';
	c->driverName[0]	= '\0';
	c->driverVer[0]		= '\0';
	c->autoCommit		= autoCommit;

	rc = SQLAllocHandle( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &c->henv );

	rc = SQLAllocHandle( SQL_HANDLE_DBC, c->henv, &c->hdbc );

	rc = SQLSetConnectAttr( c->hdbc,
		SQL_ATTR_AUTOCOMMIT,
		(SQLPOINTER)(autoCommit ? SQL_AUTOCOMMIT_ON:SQL_AUTOCOMMIT_OFF),
		SQL_NTS);

	Py_BEGIN_ALLOW_THREADS ;

	rc = SQLConnect(c->hdbc,
			(SQLCHAR *)dsn, SQL_NTS,
			(SQLCHAR *)uid, SQL_NTS,
			(SQLCHAR *)pwd, SQL_NTS
		);

	Py_END_ALLOW_THREADS ;

	if ( checkSuccess(rc) ) {
		c->connected = TRUE;
		DB2ConnObj_set_conn_info(c);	/* fill misc conn info */

		return (PyObject *)c;
	} else {
		_DB2ConnObj_Conn_Error(c, DB2_DatabaseError);
		rc = SQLFreeHandle( SQL_HANDLE_DBC, c->hdbc );
		rc = SQLFreeHandle( SQL_HANDLE_ENV, c->henv );
		Py_DECREF(c);

		return NULL;
	}
}

static void
DB2ConnObj_set_conn_info(DB2ConnObj *self)
{
	SQLRETURN	rc;
	SQLUSMALLINT	supported; /* check if SQLGetInfo() is supported */
	SQLSMALLINT	len;
	SQLHANDLE	hdbc = self->hdbc;
	int		flags = 0;

	rc = SQLGetFunctions(self->hdbc, SQL_API_SQLGETINFO, &supported);

	if (supported != SQL_TRUE) {
		return;
	}

	SQLGetInfo(hdbc, SQL_DBMS_NAME, self->dbmsName, MAX_STR_LEN, &len);
	SQLGetInfo(hdbc, SQL_DBMS_VER, self->dbmsVer, MAX_STR_LEN, &len);
	SQLGetInfo(hdbc, SQL_DRIVER_NAME, self->driverName, MAX_STR_LEN, &len);
	SQLGetInfo(hdbc, SQL_DRIVER_VER, self->driverVer, MAX_STR_LEN, &len);
	SQLGetInfo(hdbc, SQL_SERVER_NAME, self->serverName, MAX_STR_LEN, &len);
	SQLGetInfo(hdbc, SQL_FETCH_DIRECTION, &flags, 4, &len);
}

static PyObject *
_db2_SQL_type_dict(PyObject *self, PyObject *args)
{
	PyObject *d, *num, *name;
	SQLSMALLINT typeNum;
	char *typeName;
	size_t i;

	d = PyDict_New();

	for (i=0; i < sizeof(DB2SQLTypeNameMap); i++) {
		typeNum = DB2SQLTypeNameMap[i].typeNum;
		typeName = DB2SQLTypeNameMap[i].typeName;
		if (strcmp(typeName, "") == 0) {
			break;
		}
		num = PyInt_FromLong(typeNum);
		name = PyString_FromString(typeName);

		PyDict_SetItem(d, num, name);

		Py_DECREF(num);
		Py_DECREF(name);
	}

	return d;
}

/* #### CONNECTION METHODS ### */

static void
DB2ConnObj_dealloc(DB2ConnObj * self)
{
	DB2ConnObj_close( self, (PyObject *)NULL );

	PyObject_Del(self);
}

static PyObject *
DB2ConnObj_getattr(DB2ConnObj *self, char *name)
{
	if (strcmp(name, "connected") == 0) {
		return Py_BuildValue("i", self->connected);
	} else {
		return Py_FindMethod(DB2ConnObj_methods, (PyObject *)self, name);
	}
}

static PyObject *
DB2ConnObj_repr(DB2ConnObj *self)
{
	char buf[1024];
	if (self->connected) {
		sprintf(buf,
			"<open connection at %lx, "
			"serverName: %s, "
			"dbmsName: %s, "
			"dbmsVersion: %s"
			">",
			(long)self,
			self->serverName,
			self->dbmsName,
			self->dbmsVer
			);
	} else {
		sprintf(buf, "<closed connection at %lx>", (long)self);
	}

	return PyString_FromString(buf);
}

static PyObject *
DB2ConnObj_close(DB2ConnObj *self, PyObject *args)
{
/*
	.close() 
          
		Close the connection now (rather than whenever __del__ is
		called).  The connection will be unusable from this point
		forward; an Error (or subclass) exception will be raised
		if any operation is attempted with the connection. The
		same applies to all cursor objects trying to use the
		connection.  Note that closing a connection without
		committing the changes first will cause an implicit
		rollback to be performed.
*/
	if (self->connected) {
		SQLRETURN	rc;

		if (!self->autoCommit) {
			/* w/o this, SQLDisconnect will fail! */
			DB2ConnObj_rollback(self, NULL);
		}

		rc = SQLDisconnect( self->hdbc );
		CHECK_CONN_RC(rc);

		self->connected = FALSE;

		rc = SQLFreeHandle( SQL_HANDLE_DBC, self->hdbc );
		CHECK_CONN_RC(rc);

		rc = SQLFreeHandle( SQL_HANDLE_ENV, self->henv );
		CHECK_CONN_RC(rc);
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
DB2ConnObj_end_tran(DB2ConnObj *self, int endType)
{
	SQLRETURN	rc;

	if (!self->connected) {
		return _DB2ConnObj_Disconnected_Error(self);
	}

	rc = SQLEndTran( SQL_HANDLE_DBC, self->hdbc, endType );
	CHECK_CONN_RC(rc);

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
DB2ConnObj_commit(DB2ConnObj *self, PyObject * args)
{
	return DB2ConnObj_end_tran(self, SQL_COMMIT);
}

static PyObject *
DB2ConnObj_rollback(DB2ConnObj *self, PyObject *args)
{
	return DB2ConnObj_end_tran(self, SQL_ROLLBACK);
}

static PyObject *
DB2ConnObj_cursor(DB2ConnObj *self, PyObject *args)
{
	SQLRETURN	rc;
	DB2CursorObj	*c;

	if (!self->connected) {
		return _DB2ConnObj_Disconnected_Error(self);
	}

	if (!(c = PyObject_New(DB2CursorObj, &DB2CursorObj_Type))) {
		return PyErr_NoMemory();
	}

	c->hdbc		= self->hdbc;
	c->hstmt	= (SQLHANDLE)NULL;

	Py_INCREF(Py_None);
	c->description	= Py_None;

	c->bindColList	= (DB2BindStruct **)NULL;
	c->bindColCount	= 0;
	c->paramList	= (DB2ParamStruct **)NULL;
	c->paramCount	= 0;
	c->lastStmt	= (SQLCHAR *)NULL;

	c->bScrollable		= 0;
	c->lastArraySize	= -1;
	c->rowCount		= -1;
	c->opened		= 1;
	c->messages		= PyList_New(0);
	c->timeout		= 0;	/* indefinite */
	c->rowStatus		= (SQLUSMALLINT *)NULL;
	c->bFetchRefresh	= 0;

	rc = SQLAllocHandle( SQL_HANDLE_STMT, self->hdbc, &c->hstmt);

	if (checkSuccess(rc)) {
		_DB2CursorObj_timeout(c, -1);

		return (PyObject *)c;
	} else {
		Py_DECREF(c->description);
		Py_DECREF(c->messages);
		PyObject_Del(c);
		/* Connection closed ? */
		return _DB2ConnObj_Conn_Error(self, NULL);
	}
}

/* #### CURSOR METHODS ### */

static void
DB2CursorObj_dealloc(DB2CursorObj *self)
{
	DB2CursorObj_close(self, NULL);

	if (self->lastStmt) {
		MY_FREE(self->lastStmt);
		self->lastStmt = NULL;
	}

	Py_XDECREF(self->description);
	Py_XDECREF(self->messages);

	PyObject_Del(self);
}

static PyObject *
DB2CursorObj_getattr(DB2CursorObj *self, char *name)
{
	PyObject *t;

	t = Py_FindMethod(DB2CursorObj_methods, (PyObject *)self, name);
	if (t) return t;

	PyErr_Clear();
	return PyMember_Get((char *)self, DB2CursorObj_members, name);
}

static int
_DB2CursorObj_reset_cursor(DB2CursorObj * self)
{
	SQLRETURN	rc;

	rc = SQLFreeStmt(self->hstmt, SQL_RESET_PARAMS);

	if (rc == SQL_INVALID_HANDLE) {
		if (DEBUG > 1) { fprintf(stderr, "* SQL_INVALID_HANDLE\n"); }
		return 0;
	}

	rc = SQLFreeStmt(self->hstmt, SQL_UNBIND);

	rc = SQLFreeStmt(self->hstmt, SQL_CLOSE);

	/* Cursor is Scrollable or not */
	rc = SQLSetStmtAttr(self->hstmt,
		SQL_ATTR_CURSOR_SCROLLABLE,
		(SQLPOINTER)(
			self->bScrollable ? SQL_SCROLLABLE : SQL_NONSCROLLABLE
		),
		0);

	return 1;
}

static PyObject *
DB2CursorObj_close(DB2CursorObj *self, PyObject *args)
{
	SQLRETURN	rc;

	if (self->hstmt) {
		if ( _DB2CursorObj_reset_cursor(self) ) {
			rc = SQLFreeHandle( SQL_HANDLE_STMT, self->hstmt );
			CHECK_CURSOR_RC(rc);
		}
		self->hstmt = (SQLHANDLE)NULL;
	}

	_DB2CursorObj_reset_params(self, 0);
	_DB2CursorObj_reset_bind_col(self, 0);

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
DB2CursorObj_execute(DB2CursorObj *self, PyObject *args)
{
	/*

	.execute(operation[,parameters]) 
          
            Prepare and execute a database operation (query or
            command).  Parameters may be provided as sequence or
            mapping and will be bound to variables in the operation.
            Variables are specified in a database-specific notation
            (see the module's paramstyle attribute for details). [5]
            
            A reference to the operation will be retained by the
            cursor.  If the same operation object is passed in again,
            then the cursor can optimize its behavior.  This is most
            effective for algorithms where the same operation is used,
            but different parameters are bound to it (many times).
            
            For maximum efficiency when reusing an operation, it is
            best to use the setinputsizes() method to specify the
            parameter types and sizes ahead of time.  It is legal for
            a parameter to not match the predefined information; the
            implementation should compensate, possibly with a loss of
            efficiency.
            
            The parameters may also be specified as list of tuples to
            e.g. insert multiple rows in a single operation, but this
            kind of usage is depreciated: executemany() should be used
            instead.
            
            Return values are not defined.

	*/
	SQLCHAR		*stmt;
	SQLRETURN	rc;
	SQLSMALLINT	numParams = 0, numCols;
	SQLINTEGER	rowCount;
	PyObject	*params = NULL;

	if (!PyArg_ParseTuple(args, "s|O", &stmt, &params)) {
		return NULL;
	}

	/* clear .messages */
	Py_XDECREF(self->messages);
	self->messages = PyList_New(0);

	/* Close cursor (if opened) */
	if (!_DB2CursorObj_reset_cursor(self)) {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	/* Prepare */
	if (!self->lastStmt || strcmp(self->lastStmt, stmt) != 0) {
		if (SQLPrepare(self->hstmt, stmt, SQL_NTS) != SQL_SUCCESS) {
			return _DB2CursorObj_Cursor_Error(self, NULL);
		}

		if (SQLNumParams(self->hstmt, &numParams) != SQL_SUCCESS) {
			return _DB2CursorObj_Cursor_Error(self, NULL);
		}

		_DB2CursorObj_reset_params(self, numParams);

		MY_FREE(self->lastStmt);
		self->lastStmt = (SQLCHAR *)MY_MALLOC(strlen(stmt)+1);
		memset(self->lastStmt, 0, strlen(stmt)+1);
		strcpy(self->lastStmt, stmt);

		if (DEBUG) {
			fprintf(stderr,
				"* # of Params: %d for ``%s\'\'\n",
				self->paramCount, stmt);
		}
	}

	/* bind parameters */
	if (self->paramCount) {	/* There are ? markers in SQL */
		int r;

		if (self->paramCount != PyTuple_Size(params)) {
			PyObject *t;

			t = PyTuple_New(3);
			PyTuple_SetItem(t, 0, PyString_FromString(""));
			PyTuple_SetItem(t, 1, PyInt_FromLong(-1));
			PyTuple_SetItem(t, 2,
				PyString_FromString("Wrong number of parameters")
				);

			PyErr_SetObject(DB2_ProgrammingError, t);
			return NULL;
		}

		if (!_DB2CursorObj_get_param_info(self, self->paramCount)) {
			return _DB2CursorObj_Cursor_Error(self, NULL);
		}

		r = _DB2CursorObj_prepare_param_vars(self,
					self->paramCount, params);

		if (r == 0) {
			return NULL;
		} else if (r == -1) {
			return _DB2CursorObj_Cursor_Error(self, NULL);
		}
	}

	Py_BEGIN_ALLOW_THREADS ;

	rc = SQLExecute(self->hstmt);

	Py_END_ALLOW_THREADS ;

	show_rc_name("cs.execute() - SQLExecute", rc);

	if ( rc == SQL_NEED_DATA ) {
		FILE **fpPtr;

		while (1) {
			rc = SQLParamData(self->hstmt, (SQLPOINTER *)&fpPtr);
			if (rc == SQL_SUCCESS) {
				break;
			} else if (rc == SQL_NEED_DATA) {
				_DB2CursorObj_send_lob_file(self, *fpPtr);
			} else if (rc == SQL_SUCCESS_WITH_INFO) {
				_DB2CursorObj_fill_Cursor_messages(self);
			} else {
				return _DB2CursorObj_Cursor_Error(self, NULL);
			}
		}
	}
	/*
	 * SQL_NO_DATA_FOUND is returned when DB2 issues a SQL0000W warning
	 */
	if (rc == SQL_SUCCESS) {
		;
	} else if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_NO_DATA_FOUND) {
		_DB2CursorObj_fill_Cursor_messages(self);
	} else {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	/*

	.rowcount 
          
            This read-only attribute specifies the number of rows that
            the last executeXXX() produced (for DQL statements like
            'select') or affected (for DML statements like 'update' or
            'insert').
            
            The attribute is -1 in case no executeXXX() has been
            performed on the cursor or the rowcount of the last
            operation is not determinable by the interface. [7]

            Note: Future versions of the DB API specification could
            redefine the latter case to have the object return None
            instead of -1.

	*/
	rc = SQLRowCount(self->hstmt, &rowCount);
	self->rowCount = rowCount;

	self->bFetchRefresh = 1;

	if (SQLNumResultCols(self->hstmt, &numCols) == SQL_SUCCESS) {
		_DB2CursorObj_set_col_desc(self, numCols);
	} else {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	if (rowCount == -1) {
		/* SELECT */
		Py_INCREF(Py_None);
		return Py_None;
	} else {
		/* UPDATE, DELETE, INSERT, ... */
		return Py_BuildValue("i", rowCount);
	}
}

#define	MAX_PROC_PARAMS_NUM	25
#define MAX_NAME_LEN		128
#define MAX_PROC_BUF_LEN	1024

static PyObject *
DB2CursorObj_callproc(DB2CursorObj *self, PyObject *args)
{
	/*

	.callproc(procname[,parameters])
          
            (This method is optional since not all databases provide
            stored procedures. [3])
            
            Call a stored database procedure with the given name. The
            sequence of parameters must contain one entry for each
            argument that the procedure expects. The result of the
            call is returned as modified copy of the input
            sequence. Input parameters are left untouched, output and
            input/output parameters replaced with possibly new values.
            
            The procedure may also provide a result set as
            output. This must then be made available through the
            standard fetchXXX() methods.

	*/
	/* YYY */
	SQLCHAR		*procName;
	SQLCHAR		callStmt[MAX_PROC_BUF_LEN];
	PyObject	*params;
	PyObject	*retVal;

	SQLHANDLE	hstmtProc;
	SQLRETURN	rc;
	int		numParams;
	SQLSMALLINT	numCols;
	SQLINTEGER	rowCount;
	DB2ParamStruct	*ps;
	int		i;
	int		r;

	struct {
		SQLINTEGER	ind;
		SQLSMALLINT	val;
	} colType, colDataType, decDigits, nullable;

	struct {
		SQLINTEGER	ind;
		SQLCHAR		val[MAX_NAME_LEN+1];
	} colName, colDataTypeName;

	struct {
		SQLINTEGER	ind;
		SQLINTEGER	val;
	} colSize, colOrdinalPos;

	if (!PyArg_ParseTuple(args, "s|O", &procName, &params)) {
		return NULL;
	}

	_DB2CursorObj_reset_params(self, MAX_PROC_PARAMS_NUM);

	/* Close cursor (if opened) */
	_DB2CursorObj_reset_cursor(self);

	/* New handle for Stored Procedure inspection */
	rc = SQLAllocHandle(SQL_HANDLE_STMT, self->hdbc, &hstmtProc);

	rc = SQLProcedureColumns(
		hstmtProc,
		NULL,	/* CatalogName */
		0,
		"%",	/* SchemaName */
		SQL_NTS,
		procName,	/* ProcName */
		SQL_NTS,
		"%",		/* ColumnName */
		SQL_NTS
		);

	if (rc != SQL_SUCCESS) {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	rc = SQLBindCol(
		hstmtProc,
		4,		/* Column Name */
		SQL_C_CHAR,
		colName.val,
		sizeof(colName.val),
		&colName.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		5,		/* Column Type */
		SQL_C_SHORT,
		(SQLPOINTER)&colType.val,	/* IN, OUT, INOUT */
		sizeof(colType.val),
		&colType.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		6,		/* Data Type */
		SQL_C_SHORT,
		(SQLPOINTER)&colDataType.val,
		sizeof(colDataType.val),
		&colDataType.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		7,		/* Data Type Name */
		SQL_C_CHAR,
		&colDataTypeName.val,
		sizeof(colDataTypeName.val),
		&colDataTypeName.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		8,		/* Column Size */
		SQL_C_LONG,
		(SQLPOINTER)&colSize.val,
		sizeof(colSize.val),
		&colSize.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		10,		/* Decimal Digits */
		SQL_C_SHORT,
		(SQLPOINTER)&decDigits.val,
		sizeof(decDigits.val),
		&decDigits.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		12,		/* Nullable */
		SQL_C_SHORT,
		(SQLPOINTER)&nullable.val,
		sizeof(nullable.val),
		&nullable.ind
		);

	rc = SQLBindCol(
		hstmtProc,
		18,		/* Param Ordinal Position */
		SQL_C_LONG,
		(SQLPOINTER)&colOrdinalPos.val,
		sizeof(colOrdinalPos.val),
		&colOrdinalPos.ind
		);

	sprintf((char *)callStmt, "CALL %s ( ", procName);

	numParams = 0;
	i = 0;

	while (1) {
		rc = SQLFetch(hstmtProc);

		if (rc != SQL_SUCCESS) {
			break;
		}

		if (DEBUG > 1) {
			fprintf(stderr, "* Param #%d Name:: %s\n", i+1, colName.val);
		}

		strcat((char *)callStmt, "?,");

		ps = (DB2ParamStruct *) MY_MALLOC(sizeof(DB2ParamStruct));
		memset(ps, 0, sizeof(DB2ParamStruct));
		self->paramList[i] = ps;

		ps->inOutType = colType.val;	/* IN OUT */
		ps->dataType = colDataType.val;	/* SQL Data Type */
		ps->colSize = colSize.val;
		ps->decDigits = decDigits.val;
		ps->nullable = nullable.val == SQL_NULLABLE ? 1 : 0;

		i += 1;
	}

	self->paramCount = i;

	callStmt[ strlen(callStmt) - 1 ] = ')';	/* replace last , with ) */

	rc = SQLFreeHandle( SQL_HANDLE_STMT, hstmtProc );

	rc = SQLPrepare( self->hstmt, callStmt, SQL_NTS );

	if (rc != SQL_SUCCESS) {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	r = _DB2CursorObj_prepare_param_vars(self, self->paramCount, params);

	if (r == 0) {
		return NULL;
	} else if (r == -1) {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	Py_BEGIN_ALLOW_THREADS ;

	rc = SQLExecute(self->hstmt);

	Py_END_ALLOW_THREADS ;

	show_rc_name("cs.callproc() - SQLExecute", rc);

	if ( rc == SQL_NEED_DATA ) {
		FILE **fpPtr;

		while (1) {
			rc = SQLParamData(self->hstmt, (SQLPOINTER *)&fpPtr);
			if (rc == SQL_SUCCESS) {
				break;
			} else if (rc == SQL_SUCCESS_WITH_INFO ) {
				_DB2CursorObj_fill_Cursor_messages(self);
			} else if (rc == SQL_NEED_DATA) {
				_DB2CursorObj_send_lob_file(self, *fpPtr);
			} else {
				return _DB2CursorObj_Cursor_Error(self, NULL);
			}
		}
	}

	if ( rc == SQL_SUCCESS ) {
		;
	} else if ( rc == SQL_SUCCESS_WITH_INFO ) {
		_DB2CursorObj_fill_Cursor_messages(self);
	} else {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	rc = SQLRowCount(self->hstmt, (SQLINTEGER *)&rowCount);
	self->rowCount = rowCount;

	self->bFetchRefresh = 1;

	if (SQLNumResultCols(self->hstmt, &numCols) == SQL_SUCCESS) {
		_DB2CursorObj_set_col_desc(self, numCols);
	} else {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}

	/* build Return Value */
	retVal = PyTuple_New( self->paramCount );

	for ( i=0; i < self->paramCount; i++ ) {
		PyObject *tmpVal;

		ps = self->paramList[i];

		if (DEBUG > 2) {
			fprintf(stderr, "* Parameter [%d]\n", i);
			fprintf(stderr, "\tinOutType: %d\n", ps->inOutType);
			fprintf(stderr, "\tdataType: %d\n", ps->dataType);
			fprintf(stderr, "\tdataTypeName: %s\n", get_SQL_type_name(ps->dataType));
			fprintf(stderr, "\tcolSize: %d\n", (int)ps->colSize);
			fprintf(stderr, "\tbufLen: %d\n", (int)ps->bufLen);
			fprintf(stderr, "\toutLen: %d\n", (int)ps->outLen);
			fprintf(stderr, "\t---\n");
		}

		if ( ps->inOutType == SQL_PARAM_INPUT ) {
			tmpVal = PyTuple_GetItem(params, i);
			Py_INCREF(tmpVal);
		} else {
			tmpVal = _SQLType_2_PyType(ps);
		}

		PyTuple_SetItem(retVal, i, tmpVal);
	}

	return retVal;
}

static PyObject *
DB2CursorObj_fetch(DB2CursorObj *self, PyObject *args)
{
	/*

	.fetchone() 
          
            Fetch the next row of a query result set, returning a
            single sequence, or None when no more data is
            available. [6]
            
            An Error (or subclass) exception is raised if the previous
            call to executeXXX() did not produce any result set or no
            call was issued yet.

	*/
	SQLRETURN	rc;
	SQLHSTMT	hstmt = self->hstmt;
	int		wanted = -1, howmany;
	int		orient = 1;
	int		offset = 1;
	SQLUSMALLINT	fOrient;

	if (!PyArg_ParseTuple(args, "|iii", &wanted, &orient, &offset)) {
		return NULL;
	}

	if (self->bScrollable) {
		if (orient == 1) {
			fOrient = SQL_FETCH_NEXT;
		} else if (orient == 2) {
			fOrient = SQL_FETCH_FIRST;
		} else if (orient == 3) {
			fOrient = SQL_FETCH_LAST;
		} else if (orient == 4) {
			fOrient = SQL_FETCH_PRIOR;
		} else if (orient == 5) {
			fOrient = SQL_FETCH_ABSOLUTE;
		} else if (orient == 6) {
			fOrient = SQL_FETCH_RELATIVE;
		} else {
			fOrient = SQL_FETCH_NEXT;	/* fall back */
		}
	} else {
		fOrient = SQL_FETCH_NEXT;
	}

	if (wanted == -1) {
		howmany = 1;
	} else {
		howmany = wanted;
	}

	if (self->bFetchRefresh || self->lastArraySize != howmany) {
		SQLSMALLINT numCols;

		if (SQLNumResultCols(self->hstmt, &numCols) == SQL_SUCCESS) {
			_DB2CursorObj_reset_bind_col(self, numCols);
			_DB2CursorObj_bind_col(self, numCols, howmany);
		} else {
			return _DB2CursorObj_Cursor_Error(self, NULL);
		}
		self->bFetchRefresh = 0;
		self->lastArraySize = howmany;
	}

	rc = SQLSetStmtAttr(hstmt,
		SQL_ATTR_ROW_ARRAY_SIZE,
		(SQLPOINTER)howmany, 0);

	CHECK_CURSOR_RC(rc);

	rc = SQLSetStmtAttr(hstmt,
		SQL_ATTR_ROW_BIND_TYPE,
		(SQLPOINTER)SQL_BIND_BY_COLUMN, 0);

	CHECK_CURSOR_RC(rc);

	if (self->bScrollable) {
		if (0) {
		rc = SQLSetStmtAttr(hstmt,
			SQL_ATTR_CURSOR_TYPE,
			(SQLPOINTER)SQL_CURSOR_STATIC, 0);
		}
	}

	rc = SQLSetStmtAttr(hstmt,
		SQL_ATTR_ROWS_FETCHED_PTR,
		(SQLPOINTER)&self->nFetchedRows, 0);

	CHECK_CURSOR_RC(rc);

	rc = SQLSetStmtAttr(hstmt,
		SQL_ATTR_ROW_STATUS_PTR,
		(SQLPOINTER)self->rowStatus, 0);

	CHECK_CURSOR_RC(rc);

	Py_BEGIN_ALLOW_THREADS ;

	rc = SQLFetchScroll(hstmt, fOrient, (SQLINTEGER)offset);

	Py_END_ALLOW_THREADS ;

	if (checkSuccess(rc)) {
		return _DB2CursorObj_retrieve_rows(self, wanted);
	} else if (rc == SQL_NO_DATA_FOUND) {
		Py_INCREF(Py_None);
		return Py_None;
	} else {
		return _DB2CursorObj_Cursor_Error(self, NULL);
	}
}

static PyObject *
DB2CursorObj_skip_rows(DB2CursorObj *self, PyObject *args)
{
	int 		howmany = 1;
	SQLRETURN	rc;
	SQLSMALLINT	numCols;
	int		i, count = 0;

	if (!PyArg_ParseTuple(args, "|i", &howmany)) {
		return NULL;
	}

	rc = SQLNumResultCols(self->hstmt, &numCols);

	CHECK_CURSOR_RC(rc);

	if (numCols == 0) {
		return Py_BuildValue("i", -1);
	}

	for (i=0; i < howmany; i++) {
		Py_BEGIN_ALLOW_THREADS ;

		if (self->bScrollable) {
			rc = SQLFetchScroll(self->hstmt, SQL_FETCH_NEXT, 1);
		} else {
			rc = SQLFetch(self->hstmt);
		}

		Py_END_ALLOW_THREADS ;

		CHECK_CURSOR_RC(rc);

		if (rc == SQL_NO_DATA_FOUND) {
			break;
		}

		count += 1;
	}

	return Py_BuildValue("i", count);
}

static int
_DB2CursorObj_set_col_desc(DB2CursorObj *self, int numCols)
{
/*
	.description 
          
            This read-only attribute is a sequence of 7-item
            sequences.  Each of these sequences contains information
            describing one result column: (name, type_code,
            display_size, internal_size, precision, scale,
            null_ok). The first two items (name and type_code) are
            mandatory, the other five are optional and must be set to
            None if meaningfull values are not provided.

            This attribute will be None for operations that
            do not return rows or if the cursor has not had an
            operation invoked via the executeXXX() method yet.
            
            The type_code can be interpreted by comparing it to the
            Type Objects specified in the section below.
*/
	PyObject	*desc;

	/* for SQLDescribeCol() */
	SQLCHAR		colName[MAX_STR_LEN];
	SQLSMALLINT	nameLen;
	SQLSMALLINT	dataType;
	SQLUINTEGER	colSize;
	SQLSMALLINT	decDigits;
	SQLSMALLINT	nullable;

	/* for SQLColAttribute() */
	int		dispSize;
	int		internalSize;

	int		colIdx;
	int		i;

	SQLRETURN	rc;

	/* Has no output columns */
	if (numCols == 0) {
		Py_XDECREF(self->description);
		self->description = PyTuple_New(0);
		return 1;
	}

	/* Has SOME output columns */
	desc		= (PyObject *) PyTuple_New(numCols);

	for ( i = 0 ; i < numCols ; i++ ) {
		PyObject *item;

		item = PyTuple_New(7);

		colIdx = (SQLSMALLINT)(i + 1);	/* column index is 1-based */

		rc = SQLDescribeCol(self->hstmt,
			colIdx,			/* col # */
			colName,
			sizeof(colName),
			&nameLen,
			&dataType,		/* Date Type (*) */
			&colSize,		/* Precision */
			&decDigits,		/* Scale */
			&nullable		/* null OK? */
			);

		/* force *LOB --> *LOB_LOCATOR */
		if ( dataType == SQL_BLOB ) {
			dataType = SQL_BLOB_LOCATOR;
		} else if ( dataType == SQL_CLOB ) {
			dataType = SQL_CLOB_LOCATOR;
		}

		rc = SQLColAttribute(self->hstmt,
			colIdx,
			SQL_DESC_DISPLAY_SIZE,
			NULL,		/* char attr */
			0,		/* buf len */
			NULL,		/* str len */
			&dispSize	/* numeric attr */
			);

		rc = SQLColAttribute(self->hstmt,
			colIdx,
			SQL_DESC_LENGTH,
			NULL,		/* char attr */
			0,		/* buf len */
			NULL,		/* str len */
			&internalSize	/* numeric attr */
			);

		PyTuple_SetItem(item, 0, PyString_FromString(colName));
		PyTuple_SetItem(item, 1, PyInt_FromLong(dataType));
		PyTuple_SetItem(item, 2, PyInt_FromLong(dispSize));
		PyTuple_SetItem(item, 3, PyInt_FromLong(internalSize));
		PyTuple_SetItem(item, 4, PyInt_FromLong(colSize));
		PyTuple_SetItem(item, 5, PyInt_FromLong(decDigits));
		PyTuple_SetItem(item, 6, PyInt_FromLong(nullable));

		PyTuple_SetItem(desc, i, item);
	}

	Py_XDECREF(self->description);
	self->description = desc;

	return 1;
}

static int
_DB2CursorObj_reset_bind_col(DB2CursorObj *self, int numCols)
{
	int i;
	DB2BindStruct *bs;

	for (i=0; i < self->bindColCount; i++) {
		bs = self->bindColList[i];
		if (bs) {
			MY_FREE(bs->buf);
			MY_FREE(bs->outLen);
			MY_FREE(bs);
		}
	}

	if (self->bindColList) { MY_FREE(self->bindColList); }
	if (self->rowStatus) {
		MY_FREE(self->rowStatus);
		self->rowStatus = NULL;
	}

	if (numCols) {
		self->bindColList = (DB2BindStruct **) MY_MALLOC(
					sizeof(DB2BindStruct *) * numCols);
		self->bindColCount = numCols;
	} else {
		self->bindColList = (DB2BindStruct **)NULL;
		self->bindColCount = 0;
	}

	return 1;
}

static int
_DB2CursorObj_bind_col(DB2CursorObj *self, int numCols, int arraySize)
{
	int i, colIdx;
	PyObject *desc = self->description;
	PyObject *row, *col;

	SQLSMALLINT	dataType;
	SQLINTEGER	colSize, dispSize, decDigits;
	DB2BindStruct	*bCol;

	for (i=0; i < numCols; i++) {
		row = PyTuple_GetItem(desc, i);

		col = PyTuple_GetItem(row, 1);	/* 2nd: Data Type */
		dataType = (SQLSMALLINT)PyInt_AsLong(col);

		col = PyTuple_GetItem(row, 2);	/* 3th: Display Size */
		dispSize = (SQLINTEGER)PyInt_AsLong(col);

		col = PyTuple_GetItem(row, 4);	/* 5th: Column Size */
		colSize = (SQLINTEGER)PyInt_AsLong(col);

		col = PyTuple_GetItem(row, 5);	/* 6th: Decimal Digits */
		decDigits = (SQLINTEGER)PyInt_AsLong(col);

		bCol = (DB2BindStruct *) MY_MALLOC(sizeof(DB2BindStruct));
		self->bindColList[i] = bCol;

		colIdx = (SQLSMALLINT)(i + 1);

		bCol->typeEx = 0;

		switch (dataType) {

		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_VARBINARY:
		case SQL_LONGVARCHAR:
		case SQL_LONGVARBINARY:
		case SQL_BINARY:
		case SQL_DATALINK:
			bCol->type = SQL_C_CHAR;
			bCol->bufLen = sizeof(SQLCHAR)*(colSize+1);
			break;

		case SQL_DECIMAL:	/* WARNING! */
		case SQL_NUMERIC:	/* WARNING! */
			bCol->type = SQL_C_CHAR;
			bCol->typeEx = dataType;
			bCol->bufLen = sizeof(SQLCHAR)*(colSize+decDigits+1+1);
			break;

		case SQL_SMALLINT:
			bCol->type = SQL_C_SHORT;
			bCol->bufLen = sizeof(SQLSMALLINT);
			break;

		case SQL_BIGINT:
			bCol->type = SQL_C_CHAR;
			bCol->typeEx = dataType;
			/* len('-9223372036854775807L') */
			bCol->bufLen = sizeof(SQLCHAR) * 21;
			break;

		case SQL_REAL:
			bCol->type = SQL_C_FLOAT;
			bCol->bufLen = sizeof(SQLREAL);
			break;

		case SQL_INTEGER:
			bCol->type = SQL_C_LONG;
			bCol->bufLen = sizeof(SQLINTEGER);
			break;

		case SQL_DBCLOB_LOCATOR:
			bCol->type = SQL_C_DBCLOB_LOCATOR;
			bCol->bufLen = sizeof(SQLINTEGER);
			break;

		case SQL_BLOB_LOCATOR:
			bCol->type = SQL_C_BLOB_LOCATOR;
			bCol->bufLen = sizeof(SQLINTEGER);
			break;

		case SQL_CLOB_LOCATOR:
			bCol->type = SQL_C_CLOB_LOCATOR;
			bCol->bufLen = sizeof(SQLINTEGER);
			break;

		case SQL_DOUBLE:
		case SQL_FLOAT:
			bCol->type = SQL_C_DOUBLE;
			bCol->bufLen = sizeof(SQLDOUBLE);
			break;

		case SQL_TYPE_DATE:
			bCol->type = SQL_C_TYPE_DATE;
			bCol->bufLen = sizeof(DATE_STRUCT);
			break;

		case SQL_TYPE_TIME:
			bCol->type = SQL_C_TYPE_TIME;
			bCol->bufLen = sizeof(TIME_STRUCT);
			break;

		case SQL_TYPE_TIMESTAMP:
			bCol->type = SQL_C_TYPE_TIMESTAMP;
			bCol->bufLen = sizeof(TIMESTAMP_STRUCT);
			break;

		case SQL_CLOB:
		case SQL_BLOB:
			/* SQLBindFileToCol */
			if (DEBUG) printf("Oops *LOB (%i)\n", i);
			return 0;
			break;

		case SQL_DBCLOB:
		case SQL_GRAPHIC:
		case SQL_VARGRAPHIC:
		case SQL_LONGVARGRAPHIC:
			bCol->type = SQL_C_DBCHAR;
			bCol->bufLen = sizeof(SQLDBCHAR)*(colSize+1);
			break;

		default:
			bCol->type = SQL_C_CHAR;
			bCol->bufLen = sizeof(SQLCHAR)*(colSize+1);
			break;
		}

		bCol->buf = (SQLPOINTER)MY_MALLOC(bCol->bufLen * arraySize);
		memset(bCol->buf, 0, bCol->bufLen * arraySize);

		bCol->outLen = (SQLINTEGER *) MY_MALLOC(
				sizeof(SQLINTEGER) * arraySize
				);

		SQLBindCol(self->hstmt,
				colIdx,
				bCol->type,
				(SQLPOINTER)bCol->buf,
				bCol->bufLen,
				(SQLINTEGER *)bCol->outLen
			);
	}

	self->rowStatus = MY_MALLOC(sizeof(SQLUSMALLINT) * arraySize);
	memset(self->rowStatus, 0, sizeof(SQLUSMALLINT) * arraySize);

	return 1;
}

static PyObject *
_SQL_CType_2_PyType(DB2BindStruct *bs, int idx)
{
	PyObject *val, *tmpVal;
	char *tempStr;
	int size;
	SQLPOINTER buf = (SQLPOINTER)((SQLCHAR *)bs->buf + (bs->bufLen * idx));

	DATE_STRUCT dateSt;
	TIME_STRUCT timeSt;
	TIMESTAMP_STRUCT timestampSt;
	char *fractionPart;

	if ( bs->outLen[idx] == SQL_NULL_DATA ) {
		Py_INCREF(Py_None);
		return Py_None;
	}

	switch (bs->type) {

	case SQL_C_CHAR:
		switch (bs->typeEx) {

		case SQL_BIGINT:
			val = PyLong_FromString((SQLCHAR *)(buf), NULL, 0);
			break;

		case SQL_DECIMAL:	/* WARNING! */
		case SQL_NUMERIC:	/* WARNING! */
			tmpVal = PyString_FromString((SQLCHAR *)(buf));
			val = PyFloat_FromString(tmpVal, NULL);
			Py_DECREF(tmpVal);
			break;

		default:
			val = PyString_FromString((SQLCHAR *)(buf));
			break;

		}
		break;

	case SQL_C_SHORT:
		val = PyInt_FromLong(*(SQLSMALLINT *)(buf));
		break;

	case SQL_C_LONG:
		val = PyInt_FromLong(*(SQLINTEGER *)(buf));
		break;

	case SQL_C_FLOAT:
		val = PyFloat_FromDouble(*(SQLREAL *)(buf));
		break;

	case SQL_C_DOUBLE:
		val = PyFloat_FromDouble(*(SQLDOUBLE *)(buf));
		break;

	case SQL_C_BLOB_LOCATOR:
		val = PyTuple_New(2);
		PyTuple_SetItem(val, 0, PyString_FromString("blob"));
		PyTuple_SetItem(val, 1, PyInt_FromLong(*(SQLINTEGER *)(buf)));
		break;

	case SQL_C_CLOB_LOCATOR:
		val = PyTuple_New(2);
		PyTuple_SetItem(val, 0, PyString_FromString("clob"));
		PyTuple_SetItem(val, 1, PyInt_FromLong(*(SQLINTEGER *)(buf)));
		break;

	case SQL_C_TYPE_DATE:
		dateSt = *(DATE_STRUCT *)(buf);
		tempStr = (char *)MY_MALLOC(10 + 1);
		sprintf(tempStr,
				"%04d-%02d-%02d",
				dateSt.year,
				dateSt.month,
				dateSt.day
			);
		val = PyString_FromString(tempStr);
		MY_FREE(tempStr);
		break;

	case SQL_C_TYPE_TIME:
		timeSt = *(TIME_STRUCT *)(buf);
		tempStr = (char *)MY_MALLOC(8 + 1);
		sprintf(tempStr,
				"%02d:%02d:%02d",
				timeSt.hour,
				timeSt.minute,
				timeSt.second
			);
		val = PyString_FromString(tempStr);
		MY_FREE(tempStr);
		break;

	case SQL_C_TYPE_TIMESTAMP:
		timestampSt = *(TIMESTAMP_STRUCT *)(buf);
		size = 26 + 1;
		tempStr = (char *) MY_MALLOC(size);
		memset(tempStr, 0, size);
		sprintf(tempStr,
				"%04d-%02d-%02d-%02d.%02d.%02d.",
				timestampSt.year,
				timestampSt.month,
				timestampSt.day,
				timestampSt.hour,
				timestampSt.minute,
				timestampSt.second);

		/* ffffff part */

		fractionPart = (char *)MY_MALLOC(16);
		memset(fractionPart, 0, 16);
		sprintf(fractionPart, "%09d", (unsigned int)timestampSt.fraction);
		fractionPart[6] = '\0';
		strcat(tempStr, fractionPart);

		/* val = PyString_FromStringAndSize(tempStr, 26); */
		val = PyString_FromString(tempStr);

		MY_FREE(tempStr);
		MY_FREE(fractionPart);

		break;

	case SQL_C_DBCHAR:
		tempStr = (char *)MY_MALLOC(bs->outLen[idx] * 2);
		strncpy(tempStr, (char *)(buf), bs->outLen[idx] * 2);
		tempStr[ bs->outLen[idx] * 2 ] = '\0';
		val = PyString_FromString(tempStr);
		MY_FREE(tempStr);
		break;

	default:
		val = PyString_FromString((SQLCHAR *)(buf));
		break;
	}

	return val;
}

static PyObject *
_SQLType_2_PyType(DB2ParamStruct *ps)
{
	PyObject *val;
	PyObject *tmpVal;
	/*
	char *tempStr;
	*/
	void *buf = ps->buf;

	/*
	DATE_STRUCT dateSt;
	TIME_STRUCT timeSt;
	TIMESTAMP_STRUCT timestampSt;
	*/

	if ( ps->outLen == SQL_NULL_DATA ) {
		Py_INCREF(Py_None);
		return Py_None;
	}

	switch (ps->dataType) {

	case SQL_CLOB:
	case SQL_BLOB:
	case SQL_DBCLOB:
		val = PyString_FromStringAndSize((SQLCHAR *)(buf), ps->outLen);
		break;

	case SQL_BIGINT:
		val = PyLong_FromString((SQLCHAR *)buf, NULL, 0);
		break;

	case SQL_SMALLINT:
		val = PyInt_FromLong( *((SQLSMALLINT *)buf) );
		break;

	case SQL_INTEGER:
		val = PyInt_FromLong( *((SQLINTEGER *)buf) );
		break;

	case SQL_DECIMAL:
	case SQL_NUMERIC:
		tmpVal = PyString_FromStringAndSize((SQLCHAR *)(buf), ps->outLen);
		val = PyFloat_FromString(tmpVal, NULL);
		Py_DECREF(tmpVal);
		break;

	case SQL_TYPE_DATE:
	case SQL_TYPE_TIME:
	case SQL_TYPE_TIMESTAMP:
	default:
		val = PyString_FromStringAndSize((SQLCHAR *)(buf), ps->outLen);
		break;
	}

	return val;
}

static PyObject *
_DB2CursorObj_retrieve_one_row(DB2CursorObj *self, int numCols, int idx)
{
	PyObject *row;
	int i;
	PyObject *val;
	DB2BindStruct *bs;

	row = PyList_New(numCols);

	for (i=0; i < numCols; i++) {
		bs = self->bindColList[i];

		val = _SQL_CType_2_PyType(bs, idx);

		PyList_SetItem(row, i, val);
	}

	return row;
}

static PyObject *
_DB2CursorObj_retrieve_rows(DB2CursorObj *self, int howmany)
{
	SQLUINTEGER i;
	int numCols;
	PyObject *val;
	PyObject *rows;

	numCols = PyTuple_Size(self->description);

	if (howmany == -1) {
		/* fetchone() */
		return _DB2CursorObj_retrieve_one_row(self, numCols, 0);
	} else {
		SQLUSMALLINT status;

		/* fetchmany */
		rows = PyList_New(0);
		for (i=0; i < self->nFetchedRows; i++) {
			status = self->rowStatus[i];

			/* refer to SQLFetchScroll() */
			if (status == SQL_ROW_ERROR) {
				return NULL;	/* XXX */
			} else if (status == SQL_ROW_NOROW) {
				continue;
			}

			val = _DB2CursorObj_retrieve_one_row(self, numCols, i);
			PyList_Append(rows, val);
			Py_DECREF(val);
		}
		return rows;
	}
}

static int
_DB2CursorObj_reset_params(DB2CursorObj *self, int numParams)
{
	int i;
	DB2ParamStruct *ps;

	for (i=0; i < self->paramCount; i++) {
		ps = self->paramList[i];
		if (ps) {
			if (ps->bufLen) {
				MY_FREE(ps->buf);
				ps->buf = NULL;
			}
			MY_FREE(ps);
			ps = NULL;
		}
	}

	if (self->paramList) { MY_FREE(self->paramList); }

	if (numParams) {
		self->paramList = (DB2ParamStruct **)
			MY_MALLOC( sizeof(DB2ParamStruct *) * numParams );
		memset(self->paramList, 0, sizeof(DB2ParamStruct *) * numParams);
		self->paramCount = numParams;
	} else {
		self->paramList = (DB2ParamStruct **)NULL;
		self->paramCount = 0;
	}

	return 1;
}

static void
set_param_type_error(int paramIdx, SQLSMALLINT sqlType, char *requireType)
{
	char tmp[MAX_STR_LEN];

	sprintf(tmp, "Param #%d <%s> SHOULD be of type <%s>",
		paramIdx, get_SQL_type_name(sqlType), requireType);

	PyErr_SetString(PyExc_TypeError, tmp);
}

static int
_DB2CursorObj_get_param_info(DB2CursorObj *self, int numParams)
{
	int		i;
	SQLRETURN	rc;
	SQLSMALLINT	paramIdx;
	DB2ParamStruct	*ps;

	/* Get parameter info */
	for ( i=0; i < numParams; i++ ) {
		paramIdx = i + 1;
		ps = (DB2ParamStruct *) MY_MALLOC(sizeof(DB2ParamStruct));
		memset(ps, 0, sizeof(DB2ParamStruct));
		self->paramList[i] = ps;

		rc = SQLDescribeParam(
			self->hstmt,
			paramIdx,
			&ps->dataType,
			&ps->colSize,
			&ps->decDigits,
			&ps->nullable
			);

		if (!checkSuccess(rc)) {
			return 0;
		}

		ps->inOutType = SQL_PARAM_INPUT;
	}

	return 1;
}

static int
_DB2CursorObj_prepare_param_vars(DB2CursorObj *self, int numParams, PyObject *params)
{
	int		i;
	SQLRETURN	rc;
	SQLSMALLINT	paramIdx;
	DB2ParamStruct	*ps;
	PyObject	*paramVal, *tmpVal;
	SQLSMALLINT	CDataType;

	SQLSMALLINT	smallIntVal;
	SQLINTEGER	intVal;
	SQLDOUBLE       doubleVal;
	FILE		*fp;

	for ( i=0; i < numParams; i++ ) {
		paramIdx = i + 1;
		ps = self->paramList[i];

		paramVal = PyTuple_GetItem(params, i);

		if (DEBUG > 2) {
			fprintf(stderr, "* Param #%d: ColName(%s)\n", paramIdx, get_SQL_type_name(ps->dataType));
			fprintf(stderr, "* Param #%d: ColSize(%d)\n", paramIdx, (int)ps->colSize);
		}

		switch (ps->dataType) {

		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_VARBINARY:
		case SQL_LONGVARCHAR:
		case SQL_LONGVARBINARY:
		case SQL_BINARY:
		case SQL_DATALINK:

		case SQL_TYPE_DATE:		/* DATE */
		case SQL_TYPE_TIME:		/* TIME */
		case SQL_TYPE_TIMESTAMP:	/* TIMESTAMP */
			CDataType = SQL_C_CHAR;
			ps->bufLen = ps->colSize + 1;

			if ( PyString_Check(paramVal) ) {
				ps->bufLen = strlen( PyString_AsString(paramVal) ) + 1;
                ps->bufLen = (ps->colSize + 1 > ps->bufLen) ? ps->colSize + 1 : ps->bufLen;
               	ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen));
				strcpy((char *)ps->buf, PyString_AsString(paramVal) );
				ps->outLen = PyString_Size(paramVal);
			} else if ( paramVal == Py_None ) {
				ps->outLen = SQL_NULL_DATA;
				ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen));
			} else {
				set_param_type_error(paramIdx, ps->dataType, "str");
				return 0;
			}

			break;

		case SQL_CLOB:
		case SQL_BLOB:
		case SQL_DBCLOB:

			if ( PyFile_Check(paramVal) ) {
				CDataType = SQL_C_BINARY;

				ps->bufLen = ps->colSize + 1;
				ps->buf = MY_MALLOC(ps->bufLen);
				fp = PyFile_AsFile(paramVal);
				memcpy(ps->buf, &fp, sizeof(FILE *));
				ps->outLen = SQL_DATA_AT_EXEC;

			} else if ( PyString_Check(paramVal) ) {
				/* XXX: output MAY exceed */
				CDataType = SQL_C_CHAR;

				ps->bufLen = PyString_Size(paramVal);
				ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen+1));
				strcpy((char *)ps->buf, PyString_AsString(paramVal));
				ps->outLen = ps->bufLen;

			} else if ( paramVal == Py_None ) {
				CDataType = SQL_C_BINARY;

				ps->bufLen = ps->colSize + 1;
				ps->buf = MY_MALLOC(ps->bufLen);

				ps->outLen = SQL_NULL_DATA;
			} else {
				set_param_type_error(paramIdx, ps->dataType, "file | str");
				return 0;
			}

			break;

		case SQL_SMALLINT:
			CDataType = SQL_C_SHORT;

			ps->bufLen = sizeof(SQLSMALLINT);
			ps->buf = MY_MALLOC(ps->bufLen);

			if ( PyInt_Check(paramVal) ) {
				smallIntVal = (SQLSMALLINT)PyInt_AsLong(paramVal);
				memcpy(ps->buf, &smallIntVal, ps->bufLen);
				ps->outLen = ps->bufLen;
			} else if ( paramVal == Py_None ) {
				ps->outLen = SQL_NULL_DATA;
			} else {
				set_param_type_error(paramIdx, ps->dataType, "int");
				return 0;
			}

			break;

		case SQL_INTEGER:
			CDataType = SQL_C_LONG;

			ps->bufLen = sizeof(SQLINTEGER);
			ps->buf = MY_MALLOC(ps->bufLen);

			if ( PyInt_Check(paramVal) ) {
				intVal = (SQLINTEGER)PyInt_AsLong(paramVal);
				memcpy(ps->buf, &intVal, ps->bufLen);
				ps->outLen = ps->bufLen;
			} else if ( paramVal == Py_None ) {
				ps->outLen = SQL_NULL_DATA;
			} else {
				set_param_type_error(paramIdx, ps->dataType, "int");
				return 0;
			}

			break;

		case SQL_BIGINT:
			CDataType = SQL_C_CHAR;
			/*
			 * SQL_BIGINT can accept Python Int types.
			 */
			if ( PyLong_Check(paramVal) || PyInt_Check(paramVal) ) {
				tmpVal = PyObject_Str(paramVal);/* str(long) */
				ps->bufLen = PyString_Size(tmpVal);
				ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen+1));

				strcpy((char *)ps->buf, PyString_AsString(tmpVal));

				Py_DECREF(tmpVal);
				ps->outLen = ps->bufLen;
			} else if ( paramVal == Py_None ) {
				ps->bufLen = ps->colSize + ps->decDigits + 1;
				ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen+1));
				ps->outLen = SQL_NULL_DATA;
			} else {
				set_param_type_error(paramIdx, ps->dataType, "long int");
				return 0;
			}

			break;

		case SQL_REAL:
		case SQL_FLOAT:
		case SQL_DOUBLE:
			CDataType = SQL_C_DOUBLE;
			if ( PyFloat_Check(paramVal) )  {
				ps->bufLen = sizeof(SQLDOUBLE);
				ps->buf = MY_MALLOC(ps->bufLen);
				if (DEBUG) {
					fprintf(stderr, "* FLOAT\n");
				}
				doubleVal = (SQLDOUBLE)PyFloat_AsDouble(paramVal);
				if ( DEBUG ) {
					fprintf(stderr,"Double value: %f\n", doubleVal);
				}
				memcpy(ps->buf, &doubleVal, ps->bufLen);
				ps->outLen = ps->bufLen;
			}
			else if ( PyInt_Check(paramVal) ) {
				ps->bufLen = sizeof(SQLDOUBLE);
				ps->buf = MY_MALLOC(ps->bufLen);
				if (DEBUG) {
					fprintf(stderr, "* Integer to FLOAT\n");
				}
				doubleVal = (SQLDOUBLE)PyInt_AsLong(paramVal);
				memcpy(ps->buf, &doubleVal, ps->bufLen);
				ps->outLen = ps->bufLen;
			} else if ( paramVal == Py_None ) {
				ps->bufLen = sizeof(SQLDOUBLE);
				ps->buf = MY_MALLOC(ps->bufLen);
				ps->outLen = SQL_NULL_DATA;
			} else {
				set_param_type_error(paramIdx, ps->dataType, "float");
				return 0;
			}
			break;
		case SQL_DECIMAL:	/* WARNING! */
		case SQL_NUMERIC:	/* WARNING! */
		default:
			if (DEBUG) {
				fprintf(stderr, "* Param SQL Type: %d\n", ps->dataType);
			}

			CDataType = SQL_C_CHAR;

			ps->bufLen = ps->colSize + ps->decDigits + 2;
			ps->buf = MY_MALLOC(sizeof(SQLCHAR) * (ps->bufLen));

			if ( paramVal == Py_None ) {
				ps->outLen = SQL_NULL_DATA;
			} else {
				tmpVal = PyObject_Str(paramVal);
				ps->outLen = PyString_Size(tmpVal);
				strcpy((char *)ps->buf, PyString_AsString(tmpVal));
				Py_DECREF(tmpVal);
			}

			break;
		}

		rc = SQLBindParameter(
			self->hstmt,		/* Statement Handle */
			paramIdx,		/* Parameter Number */
			ps->inOutType,		/* InputOutput Type */
			CDataType,		/* Value Type */
			ps->dataType,		/* Parameter Type */
			ps->colSize,		/* Column Size */
			ps->decDigits,		/* Decimal Digits */
			(SQLPOINTER)ps->buf,	/* Parameter Value Ptr */
			ps->bufLen,		/* Buffer Length */
			&ps->outLen		/* StrLen or IndPtr */
			);

		if (rc != SQL_SUCCESS) {
			return -1;
		}
	}

	return 1;
}

static PyObject *
DB2CursorObj_read_LOB(DB2CursorObj *self, PyObject *args)
{
	SQLRETURN	rc;
	SQLINTEGER	loc, dataLen, outLen, dummyInd;
	SQLSMALLINT	locType;
	char		*lobTypeName = NULL;
	char		*buf;
	PyObject	*retData;
	SQLHANDLE	hstmt;

	if (!PyArg_ParseTuple(args, "is", &loc, &lobTypeName)) {
		return NULL;
	}
	if (strcmp(lobTypeName, "clob") == 0) {
		locType = SQL_C_CLOB_LOCATOR;
	} else if (strcmp(lobTypeName, "blob") == 0) {
		locType = SQL_C_BLOB_LOCATOR;
	} else if (strcmp(lobTypeName, "dbclob") == 0) {
		locType = SQL_C_DBCLOB_LOCATOR;
	} else {
		return NULL;
	}

	rc = SQLAllocHandle( SQL_HANDLE_STMT, self->hdbc, &hstmt);

	rc = SQLGetLength(
		hstmt,
		locType,
		loc,
		&dataLen,
		&dummyInd
		);

	if (!(retData = PyString_FromStringAndSize(NULL, dataLen))) {
		return PyErr_NoMemory();
	}

	buf = PyString_AsString(retData);

	rc = SQLGetSubString(
		hstmt,
		locType,
		loc,
		1, dataLen,
		SQL_C_BINARY,
		buf,
		dataLen,
		&outLen,
		&dummyInd
		);

	rc = SQLFreeHandle( SQL_HANDLE_STMT, hstmt);

	return retData;
}

static int
_DB2CursorObj_send_lob_file(DB2CursorObj *self, FILE *fp)
{
	SQLRETURN rc;
	char buf[1024];
	size_t count;

	/* fseek(fp, 0L, SEEK_SET); */

	while (1) {
		count = fread(buf, (size_t) 1, (size_t) 1024, fp);
		if (count == 0 || count == -1) {
			break;
		} else if (count > 0) {
			rc = SQLPutData(
				self->hstmt,
				(SQLPOINTER)buf,
				(SQLINTEGER)count);
		}
	}

	return count ? 0 : 1;
}

void
_DB2CursorObj_timeout(DB2CursorObj *self, int timeout)
{
	SQLRETURN rc;
	int timeout_2_use;

	if (timeout == -1) {
		timeout_2_use = self->timeout;
	} else {
		timeout_2_use = timeout;
	}

	rc = SQLSetStmtAttr(
			self->hstmt,
			SQL_ATTR_QUERY_TIMEOUT,
			(SQLPOINTER)timeout_2_use,
			SQL_INTEGER
			);

	show_rc_name("db.cursor() - SQLSetStmtAttr", rc);

	if (checkSuccess(rc) && timeout != -1) {
		self->timeout = timeout;
	}
}

static PyObject *
DB2CursorObj_timeout(DB2CursorObj *self, PyObject *args)
{
	int timeout;

	if (PyTuple_Size(args) == 0) {
		return Py_BuildValue("i", self->timeout);
	}

	if (!PyArg_ParseTuple(args, "i", &timeout)) {
		PyErr_SetString(PyExc_ValueError, "timeout of integer");
	}

	_DB2CursorObj_timeout(self, timeout);

	return Py_BuildValue("i", self->timeout);
}

static PyObject *
DB2CursorObj_scrollable_flag(DB2CursorObj *self, PyObject *args)
{
	PyObject *obj;

	if (PyTuple_Size(args) == 0) {
		return Py_BuildValue("i", self->bScrollable);
	}

	if (!PyArg_ParseTuple(args, "O", &obj)) {
		return NULL;
	}

	if (PyObject_IsTrue(obj)) {
		self->bScrollable = 1;
	} else {
		self->bScrollable = 0;
	}

	return Py_BuildValue("i", self->bScrollable);
}

/* ########################################################################

   Type Object

   ######################################################################### */

static PyTypeObject DB2ConnObj_Type = {
	/* type header */
#ifndef MS_WIN32
	PyObject_HEAD_INIT(&PyType_Type)
#else
	PyObject_HEAD_INIT(NULL)
#endif
	0,
	"DB2 connection",
	sizeof(DB2ConnObj),
	0,

	/* std methods */
	(destructor)DB2ConnObj_dealloc,		/* tp_dealloc */
	0,					/* tp_print */
	(getattrfunc)DB2ConnObj_getattr,
	(setattrfunc)0,
	(cmpfunc)0,
	(reprfunc)DB2ConnObj_repr,

	/* type categories */
	0,
	0,
	0,

	/* more methods */
	0,
	0,
	0,
};

static PyTypeObject DB2CursorObj_Type = {
	/* type header */
#ifndef MS_WIN32
	PyObject_HEAD_INIT(&PyType_Type)
#else
	PyObject_HEAD_INIT(NULL)
#endif
	0,
	"DB2 cursor",
	sizeof(DB2CursorObj),
	0,

	(destructor)DB2CursorObj_dealloc,
	(printfunc)0,
	(getattrfunc)DB2CursorObj_getattr,
	(setattrfunc)0,
	(cmpfunc)0,
	(reprfunc)0,

};

/* #########################################################################

   Module Method Definition

   ######################################################################### */

static PyMethodDef
_db2_methods[] = {
	{
		"connect",
		(PyCFunction)_db2_connect,
		METH_VARARGS | METH_KEYWORDS,
		"connect to DB2 instance"
	},
	{
		"get_SQL_type_dict",
		(PyCFunction)_db2_SQL_type_dict,
		METH_VARARGS,
		"get SQL type number -> name dict"
	},
	{ NULL, NULL }	/* sentinel */
};

static char _db2__doc__[] = "DB2 API for Python";

void init_DB_API_2_exception(PyObject *dict)
{
	/* ### Exception Building ### */
	/*

        StandardError
        |__Warning
        |__Error
           |__InterfaceError
           |__DatabaseError
              |__DataError
              |__OperationalError
              |__IntegrityError
              |__InternalError
              |__ProgrammingError
              |__NotSupportedError

	*/
	DB2_Error = PyErr_NewException("_db2.Error",
					PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Error", DB2_Error);

	DB2_Warning = PyErr_NewException("_db2.Warning",
					PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Warning", DB2_Warning);

	DB2_InterfaceError = PyErr_NewException("_db2.InterfaceError",
					DB2_Error, NULL);
	PyDict_SetItemString(dict, "InterfaceError", DB2_InterfaceError);

	DB2_DatabaseError = PyErr_NewException("_db2.DatabaseError",
					DB2_Error, NULL);
	PyDict_SetItemString(dict, "DatabaseError", DB2_DatabaseError);

	DB2_InternalError = PyErr_NewException("_db2.InternalError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "InternalError", DB2_InternalError);

	DB2_OperationalError = PyErr_NewException("_db2.OperationalError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "OperationalError", DB2_OperationalError);

	DB2_ProgrammingError = PyErr_NewException("_db2.ProgrammingError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "ProgrammingError", DB2_ProgrammingError);

	DB2_IntegrityError = PyErr_NewException("_db2.IntegrityError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "IntegrityError", DB2_IntegrityError);

	DB2_DataError = PyErr_NewException("_db2.DataError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "DataError", DB2_DataError);

	DB2_NotSupportedError = PyErr_NewException("_db2.NotSupportedError",
					DB2_DatabaseError, NULL);
	PyDict_SetItemString(dict, "NotSupportedError", DB2_NotSupportedError);
}

DL_EXPORT(void)
init_db2(void)
{
	PyObject *dict, *mod;
	char	*py2Debug;

	mod = Py_InitModule3("_db2", _db2_methods, _db2__doc__);

	if (!(dict = PyModule_GetDict(mod))) {
		goto Error;
	}

	init_DB_API_2_exception(dict);

	PyDict_SetItemString(dict, "__version__", PyString_FromString(__version__));

Error:
	if (PyErr_Occurred()) {
		PyErr_SetString(PyExc_ImportError, "_db2: init failure");
	}

	py2Debug = getenv("PyDB2_DEBUG");

	/* fprintf(stderr, "* ENV: %s\n", py2Debug); */

	if (py2Debug) { DEBUG = atoi(py2Debug); }

	return;
}

/* FIN */
