#!/usr/bin/env python
#coding=utf-8
'''
Created on 2012-1-15

@author: Chine
'''
import os.path

from HadoopWebManager.cmd.exceptions import CmdError
from HadoopWebManager.cmd.settings import jar_path

hadoop_base_cmd = lambda s: "hadoop %s" % s
hadoop_fs_cmd = lambda s: hadoop_base_cmd("fs %s" % s)
hadoop_jar_cmd = lambda s: hadoop_base_cmd("jar %s" % s)

generic_options = {
    'D': dict, 
    'conf': str, 
    'fs': str, 
    'jt': str, 
    'files': list, 
    'archives': list, 
    'libjars': list
    }

def parse_generic_options(options_dicts):
    if not isinstance(options_dicts, dict):
        raise CmdError('Options type must be dict')
    
    generic_options_cmd = []
    for option in options_dicts:
        if option not in generic_options:
            raise CmdError('Options must be the generic options')
        if not isinstance(option, generic_options[option]):
            raise CmdError('Options has the wrong type')
        
        if isinstance(option, str):
            generic_options_cmd.append("-%s %s" \
                                       % (option, options_dicts[option]))
        elif isinstance(option, list):
            generic_options_cmd.append("-%s %s" \
                                       % (option, ','.join(options_dicts[option])))
        elif option == "D":
            for k, v in options_dicts[option].iteritems():
                generic_options_cmd.append("-D %s=\"%s\"" \
                                           % (k, v))
    return ' '.join(generic_options_cmd)


class HDFS(object):
    @classmethod
    def ls(cls, path):
        ls_cmd = "-ls %s" % path
        return hadoop_fs_cmd(ls_cmd)
    
    @classmethod
    def cat(cls, path):
        cat_cmd = "-cat %s" % path
        return hadoop_fs_cmd(cat_cmd)
    
    @classmethod
    def rm(cls, path, recursion=False):
        rm_cmd = "-rm %s" % path if not recursion \
                    else "-rmr %s" % path
        return hadoop_fs_cmd(rm_cmd)
    
    @classmethod
    def rmr(cls, path):
        return HDFS.rm(path, True)
    
    @classmethod
    def cp(cls, src, dst):
        cp_cmd = "-cp %s %s" % (src, dst)
        return hadoop_fs_cmd(cp_cmd)
    
    @classmethod
    def mv(cls, src, dst):
        mv_cmd = "-mv %s %s" % (src, dst)
        return hadoop_fs_cmd(mv_cmd)
    
class Jar(object):
    @classmethod
    def get_cmd(cls, jar_name, **options):
        run_jar_path = ""
        jar_folder_path = ""
        if isinstance(jar_path, str):
            run_jar_path = os.path.join(jar_path, jar_name)
            jar_folder_path = jar_path
        else:
            for path in jar_path:
                run_jar_path = os.path.join(path, jar_name)
                jar_folder_path = path
                if os.path.exists(run_jar_path):
                    break
        generic_options_cmd = parse_generic_options(options)
        
        jar_cmd = "jar %s %s" % (run_jar_path, generic_options_cmd)
        
        return hadoop_jar_cmd(jar_cmd), jar_folder_path