#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    CeleryAnomal
    File Name   :    taos_client
    Author      :    Administrator
    Create Date :    2021年2月26日
    Description :    增加基于yaml配置文件启动的方式
--------------------------------------------
    Change Activity: 
        2021年2月26日 :    创建并初始化本文件
"""
import os
import sys

if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
else:
    basedir = os.path.abspath(os.getcwd() + "/../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
from my_crown.tools.config_tools import mycrown_configs
from my_crown.mycore.databases import TdEngineDatabase


# 每个线程一个独立实例为好，不保证线程安全
class TDEngineClient(TdEngineDatabase):

    def __init__(self):
        taos_conf = mycrown_configs.get("tdengine-confs")
        # 默认端口 6041，默认用户名：root,默认密码：taosdata
        conn_params = {"host": taos_conf.get('host','0.0.0.0'),
                       "rest_port": taos_conf.get('rest_port',6041),
                       "console_port": taos_conf.get('console_port',6030),
                       "user": taos_conf.get('user','root'),
                       'passwd': taos_conf.get('pswd','taosdata'),
                       "conn_type": taos_conf.get("con_type","native"),  # 执行select的时候推荐使用'rest'，否则推荐native
                       "debug": taos_conf.get("debug",False)
                       }
        super(TDEngineClient, self).__init__(taos_conf.get('database'),**conn_params)

taos_database = TDEngineClient()

def run():
    print(taos_database.get_databases())

if __name__=="__main__":
    run()

