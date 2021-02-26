#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    myCrown
    File Name   :    config_tools
    Author      :    Conley.K
    Create Date :    2021年2月25日
    Description :    yaml配置载入
--------------------------------------------
    Change Activity: 
        2021年2月25日 : 初始化
"""
import os
import shutil
import sys
import platform
import warnings  #抑制numpy警告
from configparser import ConfigParser,RawConfigParser

import yaml

basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f"{os.path.basename(__file__)} appended {basedir} into system path")
warnings.simplefilter(action='ignore', category=FutureWarning)
from loguru import logger
log_dir = os.path.abspath(basedir + "/logs/")
if not os.path.exists(log_dir):
    os.makedirs(log_dir,exist_ok=True)
log_file = os.path.abspath(log_dir+"/access.log")
logger.configure(handlers=[{"sink": sys.stdout}, {"sink":log_file, "enqueue":True, "encoding": "utf-8", "retention": "10 days", "rotation": "100MB"}])
# 载入公用的配置信息
taos_config_file = f"""{basedir}{os.sep}cfgs{os.sep}taos_configs.yaml"""
f = open(taos_config_file, encoding='utf8')
mycrown_configs = yaml.load(f, Loader=yaml.FullLoader)
f.close()

