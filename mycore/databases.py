#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    myCrown
    File Name   :    databases
    Author      :    Administrator
    Create Date :    2021/2/26
    Description :    自定义的含有 _connect 的Database对象
--------------------------------------------
    Change Activity: 
        2021/2/26 :    创建并初始化本文件
"""
import os
import sys

from core.database import Database

if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
else:
    basedir = os.path.abspath(os.getcwd() + "/../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
from mycore.connections import RestfulConn, RawConn
from tools.loguru_tools import logger

class TdEngineDatabase(Database):

    def _connect(self, db_name, **kwargs):
        conn = None
        # 优先使用本地驱动
        if self.conn_type=='native':
            try:
                # connector client 的使用条件有2，
                # 第一是在客户机安装了client，并且正确配置了hosts文件（/etc/hosts或C:\Windows\System32\drivers\etc\hosts）中的
                #   服务器域名映射和taos.cfg中的firstEP（linux：/etc/taos/taos.cfg windows：C:\TDengine\cfg\taos.cfg）
                # 其二是存在python版本的taos连接器
                if "win" in sys.platform:
                    from drivers.windows.python3 import taos
                    target_file = "taos.dll"
                    dst_dll_dir = "C:/windows/system32/"
                    dst_dll_file = f"{dst_dll_dir}{target_file}"
                    bkup_dll_path = f"{basedir}/drivers/windows/dll/"
                    # 确保依赖的dll文件存在（默认为：C:\TDengine\driver\）
                    if not os.path.exists(dst_dll_file):
                        # 将本程序自带的dll文件目录加入到path中
                        logger.warning(f"目标dll文件[{dst_dll_file}]不存在，尝试使用程序自带dll({bkup_dll_path})  ")
                        sys.path.append(bkup_dll_path)
                elif "linux" in sys.platform:
                    from drivers.linux.python3 import taos
                    # 确保依赖的so文件存在
                    target_file = f"libtaos.so.{self.driver_version}"
                    dst_dll_dir = f"/usr/local/taos/driver/"
                    dst_dll_file = f"{dst_dll_dir}{target_file}"
                    bkup_dll_path = f"{basedir}/drivers/linux/so/"
                    if not os.path.exists(dst_dll_file):
                        # 将本程序自带的so文件目录加入到path中
                        logger.warning(f"xxx 未在指定目录下[{dst_dll_file}]检测到TDEngine的so文件，尝试使用程序自带so文件({bkup_dll_path}) xxx")
                else:
                    raise RuntimeError(f"Not supported platform: [{sys.platform}]")

                conn = RawConn(db_name,**kwargs)
                logger.info(f" 正在使用taos的python连接器执行您的sql查询 ... ")
            except ConnectionError as e:
                logger.warning(f" 使用[{sys.platform}]环境下的 taos 模块连接服务器 [{kwargs.get('host')}] 失败，使用默认的Rest的方式与服务器进行通信 ")
            except Exception as e:
                logger.exception(e)
        elif self.conn_type=="rest":
            pass
        else:
            raise ValueError(f"数据库的conn_type必须为 native 或者 rest 模式，您的输入为：{self.conn_type}")
        # 否则使用rest驱动
        if conn is None:
            conn = RestfulConn(db_name, **kwargs)
            logger.info(f" 正在使用taos的 rest 连接器执行您的sql查询 ... ")
        return conn


default_database = TdEngineDatabase('demo',host='localhost')