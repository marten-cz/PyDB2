#
#	PyDB2 Distutils Setup
#	Man-Yong Lee <yong@linuxkorea.co.kr>, 2002
#

from distutils.core import setup, Extension

import sys
import os

# specifiy your DB2 root if auto-find doesn't work
# (with trailing /)
# ex) DB2_ROOT = "/opt/IBM/db2/V8.1/"
DB2_ROOT = ""

def get_db2_root():
	for v in [ '/opt/IBM/db2', '/usr/IBMdb2' ]:
		if os.path.exists(v):
			files = os.listdir(v)
			for vv in files:
				if vv[:1] == "V" and vv.find(".") > 0:
					return "%s/%s/" % (v, vv)
	else:
		return DB2_ROOT

if sys.platform[:5] == 'win32': # Win32
	db2_root_dir =  db2_root_dir = os.environ["DB2TEMPDIR"]
	db2lib = 'db2cli'
else:
	db2_root_dir = get_db2_root()
	db2lib = 'db2'

print "Your DB2 root is:", db2_root_dir

if not os.path.exists(db2_root_dir+'include/sqlcli1.h'):
	print "WARNING:",
	print "it seems that you did not install",
	print "'Application Development Kit'."
	print "Compilation may fail."
	print

setup(
	name="PyDB2",
	version="1.1.0",
	description="Python module for IBM DB2",
	author="Man-Yong (Bryan) Lee",
	author_email="manyong.lee@gmail.com",
	license="LGPL",
	package_dir={'DB2':'.'},
	py_modules=[ 'DB2' ],
	ext_modules=[
		Extension(
			"_db2",
			["_db2_module.c"],
			include_dirs=[db2_root_dir+'include'],
			library_dirs=[db2_root_dir+'lib'],
			libraries=[ db2lib ],
			)
		],
	)

# FIN

