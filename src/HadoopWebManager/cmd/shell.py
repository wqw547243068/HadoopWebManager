#!/usr/bin/env python
#coding=utf-8
'''
Created on 2012-1-15

@author: Chine
'''

import subprocess
import os.path

from HadoopWebManager.cmd.settings import hadoop_path
from HadoopWebManager.cmd import commands

class HadoopProcess(object):
    @classmethod
    def _call(cls, cmd):
        return subprocess.call(cmd, shell=True)
    
    @classmethod
    def start(cls):
        return HadoopProcess._call(os.path.join(hadoop_path, 
                                                "start-all.sh"))
    
    @classmethod
    def stop(cls):
        return HadoopProcess._call(os.path.join(hadoop_path, 
                                                "stop-all.sh"))
        
    @classmethod
    def _get_process(cls, cmd, cwd="/"):
        return subprocess.Popen(cmd, 
                            stdout=subprocess.PIPE, 
                            stderr = subprocess.PIPE,
                            cwd=cwd,
                            shell=True)
        
    class HDFS:
        @classmethod
        def ls(cls):
            pass
        
    class Jar:
        @classmethod
        def run(cls, jar_name, **options):
            run_jar_cmd, cwd = commands.Jar.get_cmd(jar_name, **options)
            HadoopProcess._get_process(run_jar_cmd, cwd)
    