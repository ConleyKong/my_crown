#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    anomaly_detector
    File Name   :    common_tools
    Author      :    Conley.K
    Create Date :    2020/5/14
    Description :    常用工具集
--------------------------------------------
    Change Activity: 
        2020/5/14 14:39 : 
"""
import os,sys,time
from functools import wraps
import warnings  #抑制numpy警告

if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../../")
else:
    basedir = os.path.abspath(os.getcwd()+"/../../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f"{os.path.basename(__file__)} appended {basedir} into system path")

warnings.simplefilter(action='ignore', category=FutureWarning)
from tools.loguru_tools import logger

__all__ = [
    'show_run_time',
]


# 运行时间统计修饰器
def show_run_time(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(">>>>> [{}] function started <<<<<".format(func.__name__ ))
        local_time = time.time()
        rlt = func(*args, **kwargs)
        logger.debug('<<<<< [{}] finished in: {:.4f}s <<<<<'.format(func.__name__ ,(time.time() - local_time)))
        return rlt
    return wrapper
