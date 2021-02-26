#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    myCrown
    File Name   :    example_basic
    Author      :    Administrator
    Create Date :    2021/2/24
    Description :    运行测试
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
    print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")
from core.crown import *
from mycore.databases import TdEngineDatabase
from mycore.taos_client import TDEngineClient
from tools.common_tools import show_run_time

DB_NAME = "demo"
conn_params = {"host": '192.168.40.140',
               "rest_port": 6041,
               "console_port": 6030,
               "user": "root",
               'passwd': 'taosdata',
               "debug":True,
               "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐 native ，rest在数据选择时可以达到default的2倍
               }
db = TdEngineDatabase(DB_NAME, **conn_params)


class Metrics(Model):
    col_float = FloatField(db_column='c_float')
    col_int = IntegerField(db_column='c_int')
    col_double = DoubleField(db_column='c_double')
    col_name = BinaryField(db_column='c_name')

    class Meta:
        database = db  # 指定表所使用的数据库
        db_table = 'tb_metrics'  # 指定表名

@show_run_time
def hello_basic():
    """ 测试直接执行的SQL语句

    :return:
    """
    DB_NAME = "demo"
    conn_params = {"host": '192.168.40.140',
                   "rest_port": 6041,
                   "console_port": 6030,
                   "user": "root",
                   'passwd': 'taosdata',
                   "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐default，rest在数据选择时可以达到default的2倍
                   }
    # 默认端口 6041，默认用户名：root,默认密码：taosdata
    db = TdEngineDatabase(DB_NAME, **conn_params)
    # db.create_database(safe=True)  # 建库 safe：如果库存在，则跳过建库指令。
    print(db.get_databases())
    print(db.get_tables())
    rlt = db.execute_sql("select * from db_demo.sub_idx_total_server_3_app_6;")
    # print(rlt.data)
    rlt = rlt.to_dataframe()
    print(rlt.head())
    # db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) #可选字段：建库时配置数据库参数，具体字段含义请参考tdengine文档。
    # db.drop_database(safe=True) #删库 safe：如果库不存在，则跳过删库指令。
    # db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156) #同建库可选字段。

def hello_taos_client():
    """ 测试直接从yaml载入配置文件初始化的taos_client

    :return:
    """
    client = TDEngineClient()
    print(client.get_databases())

def hello_create_database():
    DB_NAME = "demo"
    conn_params = {"host": '192.168.40.140',
                   "rest_port": 6041,
                   "console_port": 6030,
                   "user": "root",
                   'passwd': 'taosdata',
                   "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐 native ，rest在数据选择时可以达到default的2倍
                   }
    taos_client = TdEngineDatabase(DB_NAME, **conn_params)
    taos_client.create_database(safe=True)  # 建库指令。 （safe：如果库存在，则跳过建库指令。）
    # taos_client.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) #可选字段：建库时配置数据库参数，具体字段含义请参考tdengine文档。
    taos_client.drop_database(safe=True)  # 删库指令 （safe：如果库不存在，则跳过删库指令。）

def hello_alter_database():
    DB_NAME = "demo"
    conn_params = {"host": '192.168.40.140',
                   "rest_port": 6041,
                   "console_port": 6030,
                   "user": "root",
                   'passwd': 'taosdata',
                   "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐 native ，rest在数据选择时可以达到default的2倍
                   }
    taos_client = TdEngineDatabase(DB_NAME, **conn_params)
    taos_client.alter_database(keep=120, comp=1, replica=1, quorum=1, blocks=156)  # 同建库可选字段。

def hello_raw_sql():
    DB_NAME = "demo"
    conn_params = {"host": '192.168.40.140',
                   "rest_port": 6041,
                   "console_port": 6030,
                   "user": "root",
                   'passwd': 'taosdata',
                   "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐 native ，rest在数据选择时可以达到default的2倍
                   }
    taos_client = TdEngineDatabase(DB_NAME, **conn_params)
    # 可以通过数据库对象直接执行sql语句，语句规则与TDengine restful接口要求一致。
    res = taos_client.raw_sql('select c1,c2 from taos_test.member1')
    print(res, res.head, res.rowcount)  # 返回的对象为二维数据。res.head属性为数组对象，保存每一行数据的代表的列名。res.rowcount属性保存返回行数。
    # res: [[1.2,2.2],[1.3,2.1],[1.5,2.0],[1.6,2.1]]
    # res.head: ['c1','c2']
    # res.rowcount: 4

def hello_ORM_basic():
    """ 测试基于ORM方式的数据操作方式中常见类型数据的写入，更多类型参考如下：
    class AllField(Model):
        name_float = FloatField(column_name='n_float')  # 可选项：指定列名
        name_double = DoubleField()
        name_bigint = BigIntegerField()
        name_int = IntegerField()
        name_smallint = SmallIntegerField()
        name_tinyint = TinyIntegerField()
        name_nchar = NCharField(max_length=59)
        name_binary = BinaryField(max_length=3)
        name_ = BooleanField()
        dd = PrimaryKeyField()
        birthday = DateTimeField()

        class Meta:
            database = db_client
            db_table = 'all_field'

    :return:
    """
    DB_NAME = "demo"
    conn_params = {"host": '192.168.40.140',
                   "rest_port": 6041,
                   "console_port": 6030,
                   "user": "root",
                   'passwd': 'taosdata',
                   "conn_type": "native",  # 执行select的时候推荐使用'rest'，否则推荐 native ，rest在数据选择时可以达到default的2倍
                   }
    taos_client = TdEngineDatabase(DB_NAME,**conn_params)
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = taos_client  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名

    Metrics.create_table(safe=True)  # 建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Metrics,safe=True) #通过数据库对象建表，功能同上
    print(Metrics.table_exists())
    print(Metrics.describe_table())
    print(taos_client.curSql)  # 可以通过db对象的curSql属性获取当前执行的原始sql语句
    Metrics.drop_table(safe=True)  # 删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Metrics,safe=True) #通过数据库对象删表，功能同上
    print(Metrics.table_exists())  # 查看表是否存在，存在返回True,不存在返回：False

def hello_ORM_default_primary():
    # 不定义主键，系统默认主键：“ts”
    class TestPri(Model):
        cur = FloatField(db_column='c1')

        class Meta:
            database = db
            db_table = 'tb_demo'  # 指定表名

    TestPri.create_table()
    res = TestPri.describe_table()  # 获取表结构信息
    print(res.head)
    print(res[0])
    print(res[0][0])  # 结果: “ts”
    print(res)

def hello_ORM_primary1():
    # 定义主键方式1
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        timeline = PrimaryKeyField()  # 定义主键列，主键名设置为列名

        class Meta:
            database = db

    res = TestPri.describe_table()
    print(res[0][0])  # 主键结果: “timeline”
    print(res)


def hello_ORM_primary2():
    # 定义主键方式2,在 meta 数据中定义
    class TestPri(Model):
        cur = FloatField(db_column='c1')

        class Meta:
            database = db
            primary_key = 'timeline'  # Meta中定主键名称

    res = TestPri.describe_table()
    print(res[0][0])  # 结果: “timeline”

def hello_ORM_table():
    # 建表、删表、检查表是否存在
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = db  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名
    Metrics.create_table(safe=True)  # 建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Metrics,safe=True) #通过数据库对象建表，功能同上
    Metrics.drop_table(safe=True)  # 删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Metrics,safe=True) #通过数据库对象删表，功能同上
    Metrics.table_exists()  # 查看表是否存在，存在返回True,不存在返回：False

def hello_ORM_dynamic_create_table():
    # 动态建表
    # 除了使用定义模型类的方式建表外，还提供了动态定义字段建表的功能。
    # 可以使用Model类的类方法 dynamic_create_table 方法动态建表，第一个参数为表名，然后需要指定数据库，与是否安全建表。
    # 关键词参数可以任意多个，指定表中的字段。
    Meter_dynamic = Model.dynamic_create_table('meterD', database=db, safe=True,
                                               test1=FloatField(db_column='t1'),
                                               test2=IntegerField(db_column='t2'))
    # 函数返回的对象为Model类对象。使用方法与静态继承的模型类相同。
    Meter_dynamic.table_exists()
    Meter_dynamic.drop_table()

def hello_ORM_insert1():
    import datetime
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = db  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名

    Metrics.create_table()
    # 方法一  使用模型类实例化的每个对象对应数据表中的每一行，可以通过传入属性参数的方式给每一列赋值
    for i in range(1, 101):
        m = Metrics(col_float=1 / i, col_int=i, col_double=1 / i + 10, col_name='g1',
                   ts=datetime.datetime.now() - datetime.timedelta(seconds=(102 - i)))
        # 使用对象的save方法将数据存入数据库
        m.save()
    print(Metrics.select().count())  # 结果：100

def hello_ORM_insert2():
    import datetime
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = db  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名
    Metrics.create_table()
    # 方法二 直接使用模型类的insert方法插入数据。
    for i in range(1, 11):
        Metrics.insert(col_float=1 / i, col_int=i, col_double=1 / i + 10, col_name='g1',
                      ts=datetime.datetime.now() - datetime.timedelta(seconds=(12 - i)))
    print(Metrics.select().count())  # 结果：300
    # 如果不传入时间属性，则会以当前时刻为默认值传入
    Metrics.insert(col_float=1 / i, col_int=i, col_double=1 / i + 10, col_name='g1')
    m = Metrics(col_float=1 / i, col_int=i, col_double=1 / i + 10, col_name='g1')
    m.save()
    print(Metrics.select().count())  # 结果：300

def hello_select_one():
    # 获取一条数据
    # 使用select()类方法获取查询字段（参数留空表示取全部字段），然后可以链式使用one方法获取第一条数据
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = db  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名
    res = Metrics.select().one()
    print(res.col_name, res.col_float, res.col_int, res.col_double, res.ts)

    # select函数中可以选择要读取的字段,对于未选中的字段，对象中的对应属性将是空值
    res = Metrics.select(Metrics.col_int, Metrics.col_name).one()
    print(res.col_name, res.col_float, res.col_int, res.col_double, res.ts)

def hello_select_all():
    class Metrics(Model):
        col_float = FloatField(db_column='c_float')
        col_int = IntegerField(db_column='c_int')
        col_double = DoubleField(db_column='c_double')
        col_name = BinaryField(db_column='c_name')

        class Meta:
            database = db  # 指定表所使用的数据库
            db_table = 'tb_metrics'  # 指定表名
    # 获取所有条数据
    # 使用select()类方法获取查询字段（参数留空表示取全部字段），然后可以链式使用all方法获取全部数据
    res_all = Metrics.select().all()
    # 此时的对象为一个 QueryResultWrapper 是一个个的数据Model 对象
    for res in res_all:
        print(res.col_name, res.col_double, res.col_int, res.col_float, res.ts)

    # select函数中可以选择要读取的字段
    res_all = Metrics.select(Metrics.col_float, Metrics.col_name).all()
    for res in res_all:
        print(res.col_name, res.col_double, res.col_int, res.col_float, res.ts)

def hello_to_numpy():
    # 读取数据到numpy：
    # 通过all_raw函数可以获取二维数组格式的数据查询结果。结果每列代表的标题保存在结果对象的head属性中。
    # 此时为 SelectQuery 对象
    raw_results = Metrics.select(Metrics.col_float, Metrics.col_int, Metrics.col_double)
    raw_results = raw_results.all_raw()
    # 此时返回的是一个 Cursor 对象

    # 可以很方便的将结果转换为numpy数组对象
    # import numpy as np
    # np_data = np.array(raw_results)
    # mycrown实现的内部转换方法
    np_data = raw_results.to_ndarray()
    print(np_data)
    print(raw_results.head)

def hello_to_pandas():
    raw_results = Metrics.select().all_raw()
    # 使用以下方法，可以轻松的将数据导入pandas,并且使用时间点作为index,使用返回的数据标题作为列名。
    # pd_data = pd.DataFrame(raw_results, columns=raw_results.head).set_index('ts')
    # print(pd_data.head())
    # 我们添加的方法：
    pd_data = raw_results.to_dataframe(50)
    print(pd_data.head())

def hello_calc_on_column():
    # 直接对select得到的结果进行列的四则运算
    # # 使用select()类方法获取查询字段时，可以返回某列或多列间的值加、减、乘、除、取余计算结果（+ - * / %）
    # # 测试发现该方法不如起别名的好用，该方法的计算结果列为空
    # res_all = Metrics.select((Metrics.col_double + Metrics.col_float), Metrics.ts).all()
    # for res in res_all:
    #     itm = res.get(Metrics.col_double + Metrics.col_float)
    #     print(itm, res.ts)  # 返回的结果对象可以用get方法获取原始计算式结果


    # 推荐：可以给计算结果字段起别名
    # 此处需要重点学习下，相当于是将alias的结果作为select的字段使用
    res_all = Metrics.select(((Metrics.col_double + Metrics.col_float) * Metrics.col_double).alias('new_name'),
                            Metrics.ts).all()  # 给运算式起别名（不仅运算式，其他放在select函数中的任何属性都可以使用别名）
    for res in res_all:
        print(res.new_name, res.ts)  # 使用别名获取运算结果


def hello_where():
    # where查询条件：
    # 可以在select函数后链式调用where函数进行条件限
    one_time = datetime.datetime.now() - datetime.timedelta(hours=10)
    ress = Metrics.select().where(Metrics.ts > one_time).all()
    # 限定条件可以使用 > < == >= <= != and or ! 等。字符类型的字段可以使用 % 作为模糊查询（相当于like）
    ress = Metrics.select().where(Metrics.col_float > 0 or Metrics.col_name % 'g%').all()
    # where函数可以接收任意多参数，每个参数为一个限定条件，参数条件之间为"与"的关系。
    ress = Metrics.select().where(Metrics.col_float > 0, Metrics.ts > one_time, Metrics.col_name % '%1').all()

def hello_page_limit():
    # 分页与limit：
    # 可以在select函数后链式调用paginate函数进行分页操作，以下例子为取第6页 每页5条数据。
    ress_1 = Metrics.select().paginate(6, page_size=10).all()
    # ress_2 = Metrics.select().paginate(6).all()  # 默认page_size为20
    # 可以在select函数后链式调用limit函数和offset函数条数限制和定位操作。
    # ress_3 = Metrics.select().limit(2).offset(5).all()
    # ress_4 = Metrics.select().limit(2).all()
    for res in ress_1:
        print(res.col_name, res.col_double, res.col_int, res.col_float, res.ts)

# 意义不大，排序仅支持主键，更改的意义几乎为0
def hello_orderation():
    #排序（目前tdengine只支持主键排序）：
    # 可以在select函数后链式调用desc或者asc函数进行时间轴的正序或者倒序查询
    # 定义模型类的时候定义默认排序方法
    class Metrics(Model):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        dd = PrimaryKeyField().desc()  # 可以在定义主键的时候调用field的desc或asc方法定义默认排序

        class Meta:
            # order_by= ['-dd'] #也可以在元数据类中定义‘-dd’代表倒序‘dd’ 代表正序
            database = db

    res = Metrics.select().desc().one()

def hello_aggregation():
    # 聚合函数：
    # count
    count = Metrics.select().count()  # 统计行数
    print(count)  # 结果： 100
    count = Metrics.select().count(Metrics.col_name)  # 统计指定列非空行数
    print(count)  # 结果： 90
    # avg（sum,stddev,min,max,first,last,last_row,spread使用方法与avg相同）
    avg1 = Metrics.select().avg(Metrics.col_float, Metrics.col_double.alias('aa'))  # 可以同时获取多列，并且可以使用别名
    print(avg1.get(Metrics.col_float.avg()), avg1.aa)  # 打印统计结果
    # twa 必须配合where函数，且必须选择时间段
    twa1 = Metrics.select().where(Metrics.ts > datetime.datetime(2020, 11, 19, 15, 9, 12, 946118),
                                 Metrics.ts < datetime.datetime.now()).twa(Metrics.col_float, Metrics.col_double.alias('aa'))
    print(twa1.get(Metrics.col_float.twa()), avg1.aa)  # 打印统计结果

    # diff
    diffs = Metrics.select().diff(Metrics.col_int.alias('aa'))  # diff目前只可以聚合一个属性。
    for diff1 in diffs:
        print(diff1.aa, diff1.ts)  # 时间点数据同时返回

    # top(bottom函数使用方式相同)
    tops = Metrics.select().top(Metrics.col_float, 3, alias='aa')  # top函数需要提供要统计的属性，行数，以及别名
    for top1 in tops:
        print(top1.aa, top1.ts)  # 时间点数据同时返回
    tops = Metrics.select().top(Metrics.col_float, 3)  # 可以不指定别名
    for top1 in tops:
        print(top1.get(Metrics.col_float.top(3)))  # 不指定别名，需用使用get方法获取属性

    # percentile (apercentile函数使用方式相同) 
    percentile1 = Metrics.select().percentile((Metrics.col_float, 1, 'aa'), (
    Metrics.col_double, 2))  # 每个属性参数为一个元组（数组），分别定义要统计的属性，P值（P值取值范围0≤P≤100），可选别名。
    print(percentile1.aa)
    print(percentile1.get(Metrics.col_double.percentile(2)))  # 不指定别名，需用使用get方法获取属性

    # leastsquares
    leastsquares1 = Metrics.select().leastsquares((Metrics.col_float, 1, 1, 'aa'), (
    Metrics.col_double, 2, 2))  # 每个属性参数为一个元组（数组），分别定义要统计的属性，start_val(自变量初始值)，step_val(自变量的步长值)，可选别名。
    print(leastsquares1.aa)  # 结果： {slop:-0.001595, intercept:0.212111}
    print(leastsquares1.get(Metrics.col_double.leastsquares(2, 2)))  # 不指定别名，需用使用get方法获取属性

def hello_groupby():
    # group_by分组查询：

    # 可以在链式调用中加入group_by函数指定要分组的字段。
    # 然后在select函数中指定要分组统计的聚合函数（支持的聚合函数有：count、avg、sum 、stddev、leastsquares、percentile、min、max、first、last）
    groups = Metrics.select(Metrics.col_name, Metrics.col_int.avg().alias('intavg'),
                           Metrics.col_float.count().alias('curcount')).group_by(Metrics.col_name).all()
    for group in groups:
        print(group.desc)
        if group.col_name == 'g1':
            # assert group.get(Metrics.curInt.count()) == 10
            assert group.intavg == 5.5
            assert group.curcount == 10
        if group.col_name == 'g2':
            assert group.intavg == 10.5
            assert group.curcount == 20

def hello_interval():
    import datetime
    # 时间维度聚合interval:
    # 可以使用interval函数调用TDengine时间纬度聚合功能,使用方法如下 时间间隔与offset参数参考TDengine文档（s:秒，m:分钟，h:小时）。fill参数可选字符串(NONE | PREV | NULL | LINEAR)或者任意数值,例如：fill=1.2将会以固定值填充。
    results= Metrics.select(Metrics.col_float.avg().alias('aa'),
                            Metrics.col_float.first().alias('bb')
                            ).where(Metrics.ts > (datetime.datetime.now()-datetime.timedelta(days=1))
                                    ).interval('10s',fill='PREV',offset='5s').all()
    for result in results:
        print(result.aa,result.bb)

def hello_join():
    # join查询：
    # 目前并支持多表join查询，需要多表查询的情况请使用raw_sql函数，执行原始sql语句。以后的版本会补充此功能。
    pass

def hello_SuperTable():
    # 超级表定义：
    # 超级表模型类继承自SuperModel类
    class Meters(SuperModel):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')

        class Meta:
            database = db
            db_table = 'meters'
            # Meta类中定义的Field，为超级表的标签
            location = BinaryField(max_length=30)
            groupid = IntegerField(db_column='gid')

    # 超级表的建表、删表、检查表是否存在：
    #
    Meters.create_table(safe=True) #建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Meters,safe=True) #通过数据库对象建表，功能同上
    Meters.drop_table(safe=True) #删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Meters,safe=True) #通过数据库对象删表，功能同上
    Meters.supertable_exists() #查看表是否存在，存在返回True,不存在返回：False

    # 超级表动态建表：
    # 超级表除了使用定义模型类的方式建表外，也提供了动态定义字段建表的功能。
    #可以使用SuperModel类的类方法dynamic_create_table方法动态建表，第一个参数为表名，然后需要指定数据库，与是否安全建表
    # 需要额外提供tags参数，参数值为一个字典(使用方法如下例)，设置超级表所有的标签。
    # 关键词参数可以任意多个，指定表中的字段。
    # Meter_dynamic= SuperModel.dynamic_create_table('meterSD',database=db,safe=True,
    #                                                tags={'gid':IntegerField(db_column='tag1')},
    #                                                test1 = FloatField(db_column='t1'),
    #                                                test2 = IntegerField(db_column='t2')
    #                                                )

    # 函数返回的对象为SuperModel类对象。使用方法与静态继承的模型类相同。
    # Meter_dynamic.supertable_exists()
    # Meter_dynamic.drop_table()
    # 从超级表建立子表：

    SonTable_d3 = Meters.create_son_table('d3',location='beijing',groupid=3) #生成字表模型类的同时，自动在数据库中建表。

    SonTable_d3.table_exists() # SonTable_d3的使用方法和继承自Modle类的模型类一样。可以进行插入与查询操作
    # m = SonTable_d3(cur = 65.8,curInt=10,curDouble=1.1,desc='g1',ts = datetime.datetime.now())
    # m.save()


def hello_list():
    class AthleteList(list):
        def __init__(self, a_name, a_dob=None, a_time=[]):
            list.__init__([])
            self.name = a_name
            self.dob = a_dob
            self.extend(a_time)

    a = AthleteList('James', '1992', ["14'2''", "13'33''"])
    print(a)
    print(a[0])

def run():
    # hello_basic()
    # hello_taos_client()
    # hello_ORM_basic()
    # hello_ORM_primary1()
    # hello_ORM_primary2()
    # hello_list()
    # hello_ORM_insert1()
    # hello_ORM_insert2()
    # hello_select_one()
    # hello_select_all()
    # hello_to_numpy()
    # hello_to_pandas()
    # hello_calc_on_column()
    # hello_page_limit()
    hello_interval()

if __name__=="__main__":
    run()






