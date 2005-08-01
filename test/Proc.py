import os

pid = os.getpid()

def show():
	lines = file('/proc/%d/status' % pid).readlines()
	lines = [ x for x in lines if x.find('VmSize') >= 0 ]
	if lines:
		print '# [%d]' % pid, lines[0]

# FIN
