#!/usr/bin/env python
#coding=utf-8
'''
Created on 2012-1-15

@author: Chine
'''

# The hadoop path
hadoop_path = ""

# The path or paths where jar files exsit.
# Here, you can set it a string 
# or a list which each element is a string.
jar_path = "" # ['', ]

try:
    from local_settings import *
except ImportError:
    pass