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
            process_methods = ['rm', 'rmr', 'cp', 'mv']
            run_methods = ['ls', 'cat']
            
            if attr in process_methods:
                return HadoopProcess.HDFS._get_process(attr)
            elif attr in run_methods:
                return HadoopProcess.HDFS._call(attr)
            else:
                return super(HadoopProcess.HDFS, self).__getattribute__(attr)
        
        @classmethod
        def _get_process(cls, name):
            def inner(*args, **kwargs):
                cmd = getattr(commands.HDFS, name)(*args, **kwargs)
                return HadoopProcess._get_process(cmd)
            return inner
        
        @classmethod
        def _call(cls, name):
            def inner(*args, **kwargs):
                cmd = getattr(commands.HDFS, name)(*args, **kwargs)
                return HadoopProcess._call(cmd)
            return inner
        
    class Jar(object):
        @classmethod
        def get(cls, jar_name, **options):
            run_jar_cmd, cwd = commands.Jar.get_cmd(jar_name, **options)
            return HadoopProcess._get_process(run_jar_cmd, cwd)


class Hadoop(object):
    @classmethod
    def ls(cls, path):
        process = HadoopProcess.hdfs.ls(path)
        
        if len(process.stderr.readline()) != 0:
            return False, iter(process.stderr.readline, '')
        return True, iter(process.stdout.readline, '')
      
    @classmethod
    def cat(cls, path, start=0, end=None):
        process = HadoopProcess.hdfs.cat(path)
          
        if len(process.stderr.readline()) != 0:
            return False, iter(process.stderr.readline, '')
          
        if start <= 0 and end is None:
            return True, iter(process.stdout.readline, '')
        else:
            return True, \
                  (line for (idx, line) in enumerate(process.stdout.readline())
                   if idx >= start and idx < end)
    
    @classmethod
    def rm(cls, path):
        return True if HadoopProcess.hdfs.rm(path)==0 else False
    
    @classmethod
    def rmr(cls, path):
        return True if HadoopProcess.hdfs.rmr(path)==0 else False
    
    @classmethod
    def cp(cls, src, dst):
        return True if HadoopProcess.hdfs.cp(src, dst)==0 else False
    
    @classmethod
    def mv(cls, src, dst):
        return True if HadoopProcess.hdfs.mv(src, dst)==0 else False
                  
    @classmethod
    def jar(cls, jar_name, **options):
        return HadoopProcess.jar.get(jar_name, **options)