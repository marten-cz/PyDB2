# PyDB2
Updated PyDB2 module originaly located at https://sourceforge.net/projects/pydb2/

## Fixed bugs

**\#21** python setup.py build fails on 64bit

Building the driver on 64bit platform fails (because lib32 is used instead of lib64). On 32 bit installations there is no lib64 directory, so trying with 64 before 32 works.
