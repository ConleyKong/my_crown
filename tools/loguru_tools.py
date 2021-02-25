#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    myCrown
    File Name   :    loguru_tools
    Author      :    Conley.K
    Create Date :    2020/11/17
    Description :    统一日志输出的工具
--------------------------------------------
    Change Activity: 
        2020/11/17 10:52 : 
"""
import os
import sys
if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
else:
    basedir = os.path.abspath(os.getcwd() + "/../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f"{os.path.basename(__file__)} appended {basedir} into system path")
from loguru import logger
log_dir = os.path.abspath(basedir + "/logs/")
if not os.path.exists(log_dir):
    os.makedirs(log_dir,exist_ok=True)
log_file = os.path.abspath(log_dir+"/mycrown.log")
logger.configure(handlers=[{"sink": sys.stdout}, {"sink":log_file, "enqueue":True, "encoding": "utf-8", "retention": "7 days", "rotation": "50MB"}])
print(f">>>> Using Logfile: {log_file}")


