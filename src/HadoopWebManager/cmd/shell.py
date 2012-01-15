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
    hdfs = HDFS()
    jar = Jar
    
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
        
    class HDFS(object):
        def __getattribute__(self, attr):
            process_methods = ['ls', 'cat']
            run_methods = ['rm', 'rmr', 'cp', 'mv']
            
            if attr in process_methods:
                return HadoopProcess.HDFS._get_process(attr)
            elif attr in run_methods:
                return HadoopProcess.HDFS._call(attr)
            else:
                return super(HadoopProcess.HDFS, self).__getattribute__(attr)
        
        @classmethod
        def _get_process(cls, name):
            cmd = getattr(commands.HDFS, name)()
            return HadoopProcess._get_process(cmd)
        
        @classmethod
        def _call(cls, name):
            cmd = getattr(commands.HDFS, name)()
            return HadoopProcess._call(cmd)
        
    class Jar(object):
        @classmethod
        def get(cls, jar_name, **options):
            run_jar_cmd, cwd = commands.Jar.get_cmd(jar_name, **options)
            HadoopProcess._get_process(run_jar_cmd, cwd)
    