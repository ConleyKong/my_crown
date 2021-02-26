#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    CeleryAnomal
    File Name   :    connections
    Author      :    Administrator
    Create Date :    2021/2/24
    Description :    taos 数据库连接需要使用的连接器定义
--------------------------------------------
    Change Activity:
        2021/2/24 :    创建并初始化本文件
"""
import os
import sys
import requests
from abc import ABC, abstractmethod
if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
    if basedir not in sys.path:
        sys.path.append(basedir)
        print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
    from drivers.windows.python3 import taos
    print(">>>> running on windows ... ")
else:
    basedir = os.path.abspath(os.getcwd() + "/../")
    if basedir not in sys.path:
        sys.path.append(basedir)
        print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
    from drivers.linux.python3 import taos
    print(">>>> running on Linux ... ")
from mycore.cursors import RestfulCursor, RawCursor

class Row(list):
    def __init__(self,arr,head):
        super(Row, self).__init__(arr)
        self.head = head
    def __getitem__(self, key):
        try:
            if isinstance(key,int):
                return super(Row, self).__getitem__(key)
            else:
                return super(Row, self).__getitem__(self.head.index(key))
        except:
            return None

class AbcConn(ABC):

    @abstractmethod
    def execute_sql(self,sql):
        raise NotImplementedError

    @abstractmethod
    def cursor(self):
        raise NotImplementedError

    @abstractmethod
    def commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

class RestfulConn(AbcConn):
    fixed_path = 'rest/sql'

    def __init__(self, db_name, *args, **kwargs):
        super(RestfulConn, self).__init__()
        self.host = kwargs.get('host', "0.0.0.0")
        self.rest_port = kwargs.get('rest_port', 6041)
        self.user = kwargs.get('user', 'root')
        self.passwd = kwargs.get('passwd', 'taosdata')
        self.db_name = db_name
        self.url = f'http://{self.host}:{self.rest_port}/{self.fixed_path}'

    def execute_sql(self, sql):
        """ 发起原始sql执行的地方

        :param sql: 原始sql语句
        :return: 返回执行后的结果
        """
        reponse = requests.post(self.url, auth=(self.user, self.passwd), data=sql.encode())
        data = reponse.json()
        return data

    def cursor(self):
        """ 返回可用的cursor，该cursor需要能够执行sql语句并且可以保存执行结果

        :return:
        """
        cursor = RestfulCursor(self)
        return cursor

    def commit(self):
        pass

    def rollback(self):
        pass

class RawConn(AbcConn):

    def __init__(self, db_name, *args, **kwargs):
        super(RawConn, self).__init__()
        self.host = kwargs.get('host', "0.0.0.0")
        self.console_port = kwargs.get('console_port', 6030)
        self.user = kwargs.get('user', 'root')
        self.passwd = kwargs.get('passwd', 'taosdata')
        self.db_name = db_name
        self.core_conn = taos.connect(host=self.host,
                                      port=self.console_port,
                                      user=self.user,
                                      password=self.passwd,
                                      )

    def execute_sql(self, sql):
        """ 发起原始sql执行的地方

        :param sql: 原始sql语句
        :return: 返回带有执行结果的 原始cursor对象
        """
        cur = self.core_conn.cursor()
        cur.execute(sql)
        return cur

    def cursor(self):
        cursor = RawCursor(self)
        return cursor

    def commit(self):
        pass

    def rollback(self):
        pass

       