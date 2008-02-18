#
#    PyDB2 Distutils Setup
#    Man-Yong Lee <yong@linuxkorea.co.kr>, 2002
#    Dave Hughes - wadahu, 2007

from distutils.core import setup, Extension

import sys
import os

# Specifiy your DB2 root if auto-find doesn't work, for example:
# DB2_ROOT = "/opt/IBM/db2/V8.1/"
DB2_ROOT = ""

def find_db2rootdir():
    # Check various environment variables, depending on platform.
    if sys.platform[:5] == 'win32':
        # In Windows, DB2PATH is set to the installation path. DB2TEMPDIR is
        # usually (bizarrely) also set to the installation path, in 8dot3
        # notation
        if 'DB2PATH' in os.environ:
            print "Found DB2 with DB2PATH"
            return os.environ['DB2PATH']
        if 'DB2TEMPDIR' in os.environ:
            print "Found DB2 with DB2TEMPDIR"
            return os.environ['DB2TEMPDIR']
    else:
        # In UNIX, DB2DIR is usually set in the environment of any
        # "DB2-enabled" user (a user which has sourced
        # insthome/sqllib/db2profile). Unfortunately, it is not usually
        # exported, so is not available to the Python environment. Still, it's
        # worth a shot...
        if 'DB2DIR' in os.environ:
            print "Found DB2 with DB2DIR"
            return os.environ['DB2DIR']
    # If the above fails, attempt to run the db2level command and parse the
    # last line of output which is of the form 'Product is installed at
    # "/opt/ibm/db2/V9.1"'. The first part is localizable, so we need to
    # extract the quoted part at the end. This command should be in the path of
    # any "DB2-enabled" user (see above)
    f = os.popen('db2level')
    s = f.read()
    if not f.close() and len(s) > 0:
        s = [i.strip() for i in s.split('\n') if i.strip() != ''][-1]
        s = s[s.find('"')+1:]
        s = s[:s.rfind('"')]
        if os.path.exists(s):
            print "Found DB2 with db2level"
            return s
    # If all the above has failed, try searching some standard locations,
    # depending on platform
    if sys.platform[:5] == 'win32':
        try:
            root = os.environ['ProgramFiles']
        except KeyError:
            pass
        else:
            for path in [os.path.join(root, 'SQLLIB'), os.path.join(root, 'IBM', 'SQLLIB')]:
                if os.path.exists(path):
                    print "Found DB2 with standard location search"
                    return path
    else:
        for root in ['/opt/IBM/db2', '/opt/ibm/db2', '/usr/IBMdb2']:
            if os.path.exists(root):
                files = os.listdir(root)
                for path in files:
                    if path[:1] == "V" and path.find(".") > 0:
                        print "Found DB2 with standard location search"
                        return os.path.join(root, path)
    # Finally, try the user-specified DB2_ROOT variable
    if os.path.exists(DB2_ROOT):
        print "Found DB2 with user-specified variable"
        return DB2_ROOT
    # Give up noisily
    raise Exception('Unable to locate DB2 installation directory.\nTry editing DB2_ROOT in setup.py')

def find_db2libdir():
    # In DB2 v8 and below, the library directory is always "lib". In DB2 v9,
    # the library directory is either lib32 or lib64. According to some, a
    # "lib" symlink should point to the appropriate dir, but it certainly
    # doesn't in the installs I've seen...
    for path in ['lib', 'lib32', 'lib64']:
        if os.path.exists(os.path.join(db2rootdir, path)):
            return os.path.join(db2rootdir, path)
    # Give up noisily
    raise Exception('Unable to locate DB2 lib directory')

def find_db2lib():
    if sys.platform[:5] == 'win32':
        return 'db2cli'
    else:
        return 'db2'

db2rootdir = find_db2rootdir()
print 'DB2 install path: "%s"' % db2rootdir
db2includedir = os.path.join(db2rootdir, 'include')
print 'DB2 include path: "%s"' % db2includedir
db2libdir = find_db2libdir()
print 'DB2 lib path:     "%s"' % db2libdir
db2lib = find_db2lib()
print 'DB2 library:      "%s"' % db2lib

if not os.path.exists(os.path.join(db2includedir, 'sqlcli1.h')):
    print """WARNING:
It seems that you did not install the 'Application Development Kit'.
Compilation may fail."""

setup(
    name="PyDB2",
    version="1.1.1",
    description="Python module for IBM DB2",
    author="Man-Yong (Bryan) Lee",
    author_email="manyong.lee@gmail.com",
    url="http://sourceforge.net/projects/pydb2/",
    maintainer="Jon Thoroddsen",
    maintainer_email="jon.thoroddsen@gmail.com",
    license="LGPL",
    package_dir={'DB2': os.curdir},
    py_modules=['DB2'],
    ext_modules=[
        Extension(
            "_db2",
            ["_db2_module.c"],
            include_dirs=[db2includedir],
            library_dirs=[db2libdir],
            libraries=[db2lib],
            )
        ],
    )

# FIN

