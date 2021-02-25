#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    CeleryAnomal
    File Name   :    taos_client
    Author      :    Administrator
    Create Date :    2021/2/24
    Description :    拓展crown自带的restful方式到自带linux以及windows驱动方式
--------------------------------------------
    Change Activity: 
        2021/2/24 :    创建并初始化本文件
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


# 每个线程一个独立实例为好，不保证线程安全
class TDEngineClient(object):
    fixed_path = 'rest/sql'

    def __init__(self,database=None,**kwargs):

        self.host = kwargs.get('host')
        self.rest_port = kwargs.get('rest-port')
        self.console_port = kwargs.get('console-port')
        self.user = kwargs.get('user')
        self.passwd = kwargs.get('pswd')
        self.database = database
        self.debug = kwargs.get("developing")
        self.conn_type = kwargs.get('con_type')
        self.local_conn = None

        if self.conn_type.lower() != 'rest':
            try:
                # connector client 的使用条件有2，
                # 第一是在客户机安装了client，并且正确配置了hosts文件（/etc/hosts或C:\Windows\System32\drivers\etc\hosts）中的
                #   服务器域名映射和taos.cfg中的firstEP（linux：/etc/taos/taos.cfg windows：C:\TDengine\cfg\taos.cfg）
                # 其二是存在python版本的taos连接器
                if "win" in sys.platform:
                    from app.tdengine_client.windows.python3 import taos
                    target_file = "taos.dll"
                    dst_dll_dir = "C:/windows/system32/"
                    dst_dll_file = f"{dst_dll_dir}{target_file}"
                    bkup_dll_path = f"{basedir}tdengine_client/windows/dll/"

                    # 确保依赖的dll文件存在（默认为：C:\TDengine\driver\）
                    if not os.path.exists(dst_dll_file):
                        # 将本程序自带的dll文件目录加入到path中
                        logger.warning(f"目标dll文件[{dst_dll_file}]不存在，尝试使用程序自带dll({bkup_dll_path})  ")
                        sys.path.append(bkup_dll_path)
                elif "linux" in sys.platform:
                    from app.tdengine_client.linux.python3 import taos
                    # 确保依赖的so文件存在
                    target_file = f"libtaos.so.{taos_conf.get('version')}"
                    dst_dll_dir = f"/usr/local/taos/driver/"
                    dst_dll_file = f"{dst_dll_dir}{target_file}"
                    bkup_dll_path = f"{basedir}tdengine_client/linux/so/"
                    if not os.path.exists(dst_dll_file):
                        # 将本程序自带的so文件目录加入到path中
                        logger.warning(f"xxx 未在指定目录下[{dst_dll_file}]检测到TDEngine的so文件，尝试使用程序自带so文件({bkup_dll_path}) xxx")
                else:
                    raise RuntimeError(f"Not supported platform: [{sys.platform}]")

                # todo 此处的配置文件需要在linux平台上进行调优,需要查阅相关connector的配置说明
                self.local_conn = taos.connect(host=self.host,
                                               port=self.console_port,
                                               user=self.user,
                                               password=self.passwd,
                                               # config="/etc/taos"
                                               )
                logger.info(f" 正在使用taos的python连接器执行您的sql查询 ... ")
            except ConnectionError as e:
                logger.warning(f" 使用[{sys.platform}]环境下的 taos 模块连接服务器 [{self.host}] 失败，使用默认的Rest的方式与服务器进行通信 ")
            except Exception as e:
                logger.exception(e)
                exit(-1)

        self.url = f'http://{self.host}:{self.rest_port}/{self.fixed_path}'

        self.create_database()

    def close(self):
        if self.local_conn:
            self.local_conn.close()

    def execute_sql(self,sql,format='json'):
        """

        :param sql: 待执行的sql语句
        :param format: 返回数据的格式，默认为json串，支持dataframe的格式，
            dataframe格式仅对返回数据的sql有效，对于无数据返回的情况，Dataframe默认返回None
        :return: 返回查询或者处理后的数据
        """
        if self.debug:
            logger.debug(f"executing >>> {sql}")
        try:
            if self.local_conn:
                local_cursor = self.local_conn.cursor()
                local_cursor.execute(sql)
                affects = local_cursor.affected_rows
                cols = [x[0] for x in local_cursor.description]
                rows = 0
                rsp = {'affected_rows':affects,'head':cols,'rows':rows}
                if len(cols)>0:
                    data = local_cursor.fetchall()  # Use fetchall to fetch data in a list
                    rows = len(data)
                    rsp['rows']=rows
                    rsp['data']=data
                local_cursor.close()
                if format.lower() =='dataframe':
                    return self.convert_dataframe(rsp)
                else:
                    return json.dumps(rsp,cls=ComplexEncoder,ensure_ascii=False)
            else:
                reponse = requests.post(self.url, auth=(self.user, self.passwd),data=sql.encode())
                data = reponse.json()  # 返回的结果本质是一个dict
                if data.get('status') != 'succ':
                    raise ValueError(f"TSQL-ERROR raised while executing \n[ {sql} ] \nraised TSQL error: {json.dumps(data)}")
                else:
                    if format.lower() == 'dataframe':
                        return self.convert_dataframe(data)
                    else:
                        return json.dumps(data, cls=ComplexEncoder, ensure_ascii=False)

        except Exception as e:
            self.close()
            raise e
        finally:
            if local_cursor:
                local_cursor.close()


    def convert_dataframe(self,taos_body):
        import pandas as pd
        head = taos_body.get('head')
        data = taos_body.get('data')
        df = pd.DataFrame(data,columns=head)
        if 'ts' in df.columns:
            df['ts'] = pd.to_datetime(df['ts'])
        return df


    def use_database(self,database=None):
        """ 指定使用的数据库，也可以在建立连接时指定

        :param database: 目标数据库名称，不指定则使用类类变量
        :return:
        """
        database = self.database if database is None else database
        sql = f"use {database};"
        return self.execute_sql(sql)


    def create_database(self,safe=True):
        create_sql = f"create database "
        if safe:
            create_sql += "if not exists "
        create_sql +=f" {self.database} ;"
        rsp = self.execute_sql(create_sql)
        use_sql = f"use {self.database}; "
        self.execute_sql(use_sql)
        return rsp

    def delete_database(self,safe=True):
        print(f"deleting database: [{self.database}]")
        drop_sql = "drop database "
        if safe:
            drop_sql += " if exists "
        drop_sql += f" {self.database}"
        rsp = self.execute_sql(drop_sql)
        return rsp

    def get_server_status(self):
        """ 检查服务器状态

        :return:
        """
        return self.execute_sql("SELECT SERVER_STATUS() AS status;")

    def get_server_version(self):
        """ 查询服务器版本

        :return:
        """
        return self.execute_sql("SELECT SERVER_VERSION() AS server_version;")

    def get_client_version(self):
        """ 获取客户端版本

        :return:
        """
        return self.execute_sql("SELECT CLIENT_VERSION() AS client_version;")

    def get_databases(self):
        """ 显示出所有数据库名

        :return:
        """
        return self.execute_sql("SELECT DATABASE();")

