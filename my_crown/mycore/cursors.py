#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    CeleryAnomal
    File Name   :    cursors
    Author      :    Administrator
    Create Date :    2021/2/24
    Description :    taos 数据库连接需要使用的 cursor 定义
--------------------------------------------
    Change Activity:
        2021/2/24 :    创建并初始化本文件
"""
import os
import sys
from abc import abstractmethod
from datetime import datetime

if "win" in sys.platform:
    basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
    if basedir not in sys.path:
        sys.path.append(basedir)
        print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
else:
    basedir = os.path.abspath(os.getcwd() + "/../")
    if basedir not in sys.path:
        sys.path.append(basedir)
        print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")

class BaseCursor(list):
    def __init__(self, conn):
        super(BaseCursor, self).__init__([])
        self.conn = conn
        self.status = ''
        self.affected_rows = 0
        self.rowcount = 0
        self.head = []
        self.data = []
        self.dataframe = None
        self.ndarray = None
        self.err_code = None
        self.err_desc = ''

    def set_data(self,data=[]):
        self.clear()
        self.extend(data)

    def to_dataframe(self,max_len:int=None,reset=True):
        """ 将数据中的前多少行构建为Dataframe

        :param max_len: 行数，默认为全部
        :param reset: 是否将当次转换结果覆盖类内缓存
        :return: 返回生成的dataframe对象
        """
        import pandas as pd
        assert len(self.head) == len(self.data[0]),"转换dataframe时无法确保head和data维度一致  "
        if max_len:
            dataframe = pd.DataFrame(self[:int(max_len)], columns=self.head)
        else:
            dataframe = pd.DataFrame(self, columns=self.head)
        # 默认 'ts' 为时间主键 ，若不存在则以第一个为主键
        if 'ts' in dataframe.columns:
            # df['ts'] = pd.to_datetime(df['ts'])
            dataframe.set_index('ts')
        else:
            pri_key = self.head[0]
            dataframe.set_index(pri_key)

        if reset:
            self.dataframe = dataframe
        return dataframe

    def to_ndarray(self,max_len:int=None,reset=True):
        """ 将数据中的前多少行转换为Ndarray对象

        :param max_len: 行数，默认为全部
        :param reset: 是否将当次转换结果覆盖类内缓存
        :return: 返回生成的Ndarray对象
        """
        import numpy as np
        if max_len:
            ndarray = np.array(self[:int(max_len)])
        else:
            ndarray = np.array(self)
        if reset:
            self.ndarray = ndarray
        return ndarray



    @abstractmethod
    def execute(self,sql,param=()):
        raise NotImplementedError

    def __str__(self):
        df = self.to_dataframe(20,False)
        return str(df.head())

# 基于RestFul执行的Cursor是执行taos-sql的最基础类，不但具有执行功能还需要能够记录执行结果
class RestfulCursor(BaseCursor):
    """ Cursor对象除了带有execute功能外还需要能够承载执行后的结果
     * 含有一个conn对象，因此可以执行conn的execute
     * 当前对象属于 list 的子类，能够进行 索引操作？

    """
    def __init__(self,conn):
        BaseCursor.__init__(self, conn)

    def execute(self,sql,param=()):
        """ 真正执行SQL并进行数据后处理的地方,执行的结果将会记录在当前的对象中


        :param sql: 带执行的SQL语句，支持动态参数
        :param param: 待填补的参数值
        :return: 语句执行成功则返回True并将结果保存在当前对象中（若数据库不存在会尝试创建一次数据库后再执行slq语句），
                否则抛出错误
        """
        if param:
            param = map(lambda x: '"%s"' % x if isinstance(x,str) or isinstance(x,datetime) else x, param)
            sql = sql.format(*param)
        res = self.conn.execute_sql(sql)
        if res:
            self.status=res.get('status')
            if self.status == 'succ':
                self.rowcount = res.get('rows')
                self.head = res.get('head')
                self.data = res.get('data')
                self.set_data(self.data)  # 当前是list的子对象，因此可以用于初始化list
                return True
            else:
                self.err_code = res.get('code')
                self.err_desc = res.get('desc')
                #自动尝试建立数据库并重新执行
                if self.err_code == 896:
                    res = self.execute(f"create database {self.conn.db_name}")
                    if res:
                        return self.execute(sql)
                raise Exception(f" While executing \n[ {sql} ] \nTSQL exception occured: {self.err_desc}")
        else:
            raise Exception(f'server connect executing error on database: {self.conn.db_name}')

# 原生的 Cursor
class RawCursor(BaseCursor):

    def __init__(self,conn):
        super(RawCursor, self).__init__(conn)
        self.local_cursor = None

    def execute(self, sql, param=()):
        """ 真正执行SQL并进行数据后处理的地方,执行的结果将会记录在当前的对象中


        :param sql: 带执行的SQL语句，支持动态参数
        :param param: 待填补的参数值
        :return: 语句执行成功则返回True并将结果保存在当前对象中（若数据库不存在会尝试创建一次数据库后再执行slq语句），
                否则抛出错误
        """
        if param:
            param = map(lambda x: '"%s"' % x if isinstance(x, str) or isinstance(x, datetime) else x, param)
            sql = sql.format(*param)
        local_cursor = self.conn.execute_sql(sql)
        if local_cursor:
            cols = [x[0] for x in local_cursor.description]
            self.affected_rows = local_cursor.affected_rows
            self.head = cols
            if len(cols) > 0:  # 是select语句  #否则是 update或者insert语句
                # data = local_cursor.fetchall()  # Use fetchall to fetch data in a list
                data = local_cursor.fetchall_block() # fetchall内部使用的for循环，效率很低，fetchall_block进行了分批，速度更快一点
                rows = len(data)
                self.rowcount = rows
                self.data = data
                self.set_data(self.data)  # 当前是list的子对象，因此可以用于初始化list
                # super(RawCursor, self).__init__(self.data)  # 当前是list的子对象，因此可以用于初始化list
            return True
        else:
            raise Exception(f'server connect executing error on database: {self.conn.db_name}')

