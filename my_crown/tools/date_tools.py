#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    kubeflow_traceanomal
    File Name   :    date_tools
    Author      :    Conley.K
    Create Date :    2020/10/13
    Description :    日期处理工具函数包
--------------------------------------------
    Change Activity: 
        2020/10/13 18:01 : 
"""
import json
import os
import sys
import collections
from datetime import datetime, timedelta, timezone, date
import dateutil.parser as dateparser

import numpy as np

basedir = os.path.abspath(os.path.dirname(__file__) + "/../")
if basedir not in sys.path:
    sys.path.append(basedir)
    print(f">>>> {os.path.basename(__file__)} appended {basedir} into system path")

# 毫秒转整数的映射关系
msec_repo={'a':1,'s':1000,'m':60*1000,'h':60*60*1000,'d':24*60*60*1000,'w':7*24*60*60*1000}

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def assert_timestamp(ts:int):
    """ 确认时间戳为ms级别，若小于ms或者超过ms都会报错

    :param ts: 给入的时间戳int值
    :return: 返回ms级别时间戳，若转换出错则抛出异常
    """
    if 1e12<ts<1e13:
        return ts
    elif 1<=ts//1e9<10:
        ts = ts * 1000
    if 1e12<ts<1e13:
        return ts
    else:
        raise ValueError(f"时间戳必须能够转化为13位ms级别时间戳信息: {ts}")

def msec_timestep(step_str:str)->int:
    """ 将传入的时间跨度转换为毫秒数

    :param step_str: 开头为数字，末尾为一个字符的跨度单位，跨度单位支持： a(毫秒)、s(秒)、 m(分)、h(小时)、d(天)、w(周)
    :return: 返回该跨度对应的毫秒数,整数
    """
    global msec_repo
    step_str = step_str.strip()
    unit_str = step_str[-1]
    _number = float(step_str[:-1].strip())
    msec_num = int(_number*msec_repo.get(unit_str))
    return msec_num

def convert_datestr_to_timestamp(date_str,prec="s")->int:
    """ 将日期字符串转换为时间戳整数

    :param date_str: 日期字符串
    :param prec: 精确度，s返回10位数，ms返回13位
    :return: 返回10位时间戳（s级别），整数
    """
    ts = dateparser.parse(date_str).timestamp()
    if prec == 'ms':
        ts = ts*1000
    return int(ts)

# 判断字符串是数字
def is_number(num):
    import re
    pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*|[-+]?\.?[0-9]\d*$')
    result = pattern.match(num)
    if result:
        return True
    else:
        return False

def convert_utc_to_beijing(date_str,return_format="standard")->str:
    """ 将utc时间转换为北京东八区时间

    :param date_str: utc时间串
    :param return_format: 返回的时间串格式，iso格式或者standard模式，前者包含T
    :return:
    """
    _beijing_date = ""
    try:
        _utc_dt = dateparser.parse(date_str).replace(tzinfo=timezone.utc)
        _beijing_date = _utc_dt.astimezone(timezone(timedelta(hours=8)))
        if return_format=="iso":
            _beijing_date = _beijing_date.isoformat()
        else:
            _beijing_date = str(_beijing_date)
    except Exception as e:
        print(e)

    return _beijing_date

def convert_beijing_to_utc(date_str,return_format="standard"):
    """ 将北京时间转换为utc时间串

    :param date_str: 东八区时间串
    :param return_format: 返回的时间串格式，iso格式或者standard模式，前者包含T
    :return:
    """
    _utc_date = ""
    try:
        _beijing_date = dateparser.parse(date_str).replace(tzinfo=timezone(timedelta(hours=8)))
        _utc_date = _beijing_date.astimezone(timezone(timedelta(hours=0)))
        if return_format == "iso":
            _utc_date = _utc_date.isoformat()
        else:
            _utc_date = str(_utc_date)
    except Exception as e:
        print(e)

    return _utc_date

def get_beijing_datetime(dateformat="standard")->str:
    """ 获取当前的北京时间

    :param dateformat: 支持iso格式和标准格式，其中iso格式会加一个T并在末尾加上时区
    :return:
    """
    # 拿到UTC时间，并强制设置时区为UTC+0:00:
    utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    # astimezone()将转换时区为北京时间: 2020-09-03 15:35:44.427904+08:00，
    # datetime类型
    beijing_date = utc_dt.astimezone(timezone(timedelta(hours=8)))

    if dateformat=="iso":  # 2020-09-03 15:35:44， str类型
        beijing_date = beijing_date.isoformat()
    else:
        beijing_date = str(beijing_date)

    return beijing_date


def get_datetime_beijing(standard_format=True):
    """ 获取当前的北京时间

    :param standard_format: 是否将时间戳格式化为“%Y-%m-%d %H:%M:%S”
    :return: 返回datetime,若standard_format为True则返回字符串格式
    """
    # 拿到UTC时间，并强制设置时区为UTC+0:00:
    utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    # astimezone()将转换时区为北京时间: 2020-09-03 15:35:44.427904+08:00，
    # datetime类型
    beijing_date = utc_dt.astimezone(timezone(timedelta(hours=8)))

    if standard_format:  # 2020-09-03 15:35:44， str类型
        beijing_date = beijing_date.strftime("%Y-%m-%d %H:%M:%S")

    return beijing_date


def resort_timeseries(dataFrame,timecolumn="ds",
                      required_columns=(),
                      time_as_index=False,
                      assending=True):
    """ 将指定的DataFrame重新按照时间排序后返回

    :param dataFrame: 仅支持pandas.DataFrame格式数据
    :param timecolumn: 时间列名称，默认为“ds”
    :param required_columns: 必须包含的数据列，默认是“ds”和“y”列
    :param time_as_index: 是否在返回的数据中将时间列作为索引
    :param assending: 升序排序，默认为True
    :return: 返回根据timecolumn列排序后数据
    """
    required_columns = list(required_columns)
    if timecolumn not in required_columns:
        required_columns.append(timecolumn)
    if required_columns is not None and len(required_columns)>0:
        columns = dataFrame.columns.values
        _errs = []
        err_flag = False
        for c in required_columns:
            if c not in columns :
                _errs.append("{}")
                err_flag = True
        if err_flag:
            err_msg = " and ".join(_errs)
            err_msg +=" column must in input data!"
            err_msg = err_msg.format(_errs)
            raise ValueError(err_msg)
    dataFrame = dataFrame.sort_values(by=timecolumn,ascending=assending)
    dataFrame.reset_index(inplace=True)
    del dataFrame['index']
    if time_as_index:  #用于InfluxDB插入时间序列数据，提供时间索引类型
        ts_dates = dataFrame[timecolumn].values
        if is_number(str(ts_dates[0])):
            dataFrame[timecolumn] = convert_timestamp2datetime64s(ts_dates)
        else:
            dataFrame[timecolumn] = np.array(ts_dates, dtype=np.datetime64)
            # _tmp = pd.to_datetime(ts_dates)
            # dataFrame[timecolumn] = pd.to_datetime(ts_dates).values.astype("datetime64[s]")

        dataFrame.set_index(timecolumn, inplace=True) #此时timecolumn不再是独立的列

    return dataFrame


def convert_timestamp2datetime64(timestamp_value):
    """ 将单值从int时间戳转换为np.datetime64时间格式

    :param timestamp_value:
    :return:
    """
    import pytz
    timestamp_value = int(timestamp_value)
    if timestamp_value>9999999999:
        timestamp_value = int(timestamp_value/1000)
    t = datetime.fromtimestamp(timestamp_value, pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%dT%H:%M:%S')

    return np.datetime64(t)



def convert_timestamp2datetime64s(timestamp_values):
    """ 批量转换时间戳到datetime64

    :param timestamp_values:
    :return:
    """
    import pandas as pd
    timestamp_values = np.array(timestamp_values)
    timestamp_value = int(timestamp_values[0])
    if timestamp_value > 9999999999:
        timestamp_values = np.array(timestamp_values / 1000,dtype=int)

    ds = pd.to_datetime(timestamp_values, utc=True, unit='s').tz_convert('Asia/Shanghai').values.astype("datetime64[s]")
    return ds

# 自适应获取最优显示单位和步长
def get_date_showunit(timeseries_dates, max_show_step=15):
    """ 基于时间序列的长度决定在显示时使用Y还是M还是D或h/m/s

    :param timeseries_dates: 待判定时间长度显示单位和跨度的
    :param max_show_step: 横轴最多显示的时间标识数量
    :return: 显示用的单位show_unit（Y/M/D/h/m/s）,步长的单位长度数量 show_span
    """
    start_date = timeseries_dates[0]
    end_date = timeseries_dates[-1]
    total_span = end_date - start_date
    # 默认图片能显示60个单位，3个单位一个label能显示20个label unit:(timedelta,showspan)
    DateUnitDict = collections.OrderedDict({"Y": np.timedelta64(60, "M"),  # 5年
                                            "M": np.timedelta64(60, "D"),  # 2月
                                            "D": np.timedelta64(60, "h"),  # 3天
                                            "h": np.timedelta64(60, "m"),  # 1小时
                                            "m": np.timedelta64(60, "s")  # 1m
                                            })
    show_unit = "s"
    for unit_key in DateUnitDict:
        _threshold = DateUnitDict.get(unit_key)
        if total_span > _threshold.astype(total_span.dtype):
            show_unit = unit_key
            break

    _span_unit = total_span / np.timedelta64(1, show_unit).astype(total_span.dtype)
    show_span = int((_span_unit+max_show_step-1) // max_show_step)

    return show_unit, show_span

def get_period_name(name):
    mapper = {"Y": "month_of_year_effect",
              "M": "day_of_month_effect",
              "W": "day_of_week_effect",
              "D": "hour_of_day_effect",
              "h": "minute_of_hour_effect"}
    return mapper.get(name,str(name))

def get_readable_name(datetype_str):
    date_unit_dict = {"datetime64[Y]": "Y",
                      "datetime64[M]": "M",
                      "datetime64[D]": "D",
                      "datetime64[h]": "h",
                      "datetime64[m]": "m",
                      "datetime64[s]": "s",
                      "datetime64[ms]": "ms",
                      }
    timeseries_unit = None
    for str_unit in date_unit_dict.keys():
        if str_unit == datetype_str:
            timeseries_unit = date_unit_dict.get(str_unit)
            break

    return timeseries_unit

def get_timeseries_stepunit(timeseries_dates,auto_scale=False):
    """ 计算并返回时间序列的可读单位以及平均间距长度

    :param timeseries_dates: 时间列表，np.datetime64类型
    :param auto_scale: 是否自动对数据跨度单位进行缩放
    :return: 时间步长(int)，时间单位(['s','D'等]),时间序列的跨度（timedelta64)
    """
    start_date = timeseries_dates[0]
    end_date = timeseries_dates[-1]
    total_span = end_date - start_date

    # timeseries_unit = date_unit_dict.get(start_date.dtype) #宏变量关系，无法直接根据key-value取到
    time_unit = get_readable_name(start_date.dtype) # 时间单位
    if time_unit is None:
        raise Exception("  Warning: 时间单位小于ms或者识别失败，无法计算时长以及单位等信息...")

    time_step = int(round(total_span / (np.timedelta64(1, time_unit)) / len(timeseries_dates)))

    if auto_scale:
        time_step, time_unit, total_span = autoscale_timestep_unit(time_step,time_unit,total_span)

    return time_step,time_unit,total_span

def autoscale_timestep_unit(time_step, time_unit,total_span=None, use_week=False):
    """ 对于给入的时间步长与时间单位进行自动缩放，比如跨度为60s的就修改为1min

    :param time_step: 原始时间步长
    :param time_unit: 原始时间步长单位，如s,m,h,D,M,Y
    :param total_span: 原总时间长度，timedelta64 类型,可选，若传入则该值也做相应的缩放,
                       对于不确定是否长度是否大于一个周期的可传入该参数，若该值输出为1则不适合auto_scale
    :param use_week: 是否启用周类型数据类型，默认不启用，W
    :return: 自动缩放后的时间步长与时间单位，若传入了total_span则返回值也包含该值
    """
    #todo 如果大于阈值但是小于下一个阈值则返回
    scale_params = [{"s":{"thd":59,"period":60,"next":"m"}},
                    {"m": {"thd": 59, "period": 60,"next":"h"}},
                    {"h": {"thd": 23, "period": 24,"next":"D"}},
                    {"D": {"thd": 364, "period": 365, "next": "Y"}},
                    {"D": {"thd": 28, "period": 30, "next": "M"}},
                    {"D": {"thd": 6, "period": 7, "next": "W"}},
                    {"M": {"thd": 11, "period": 12, "next": "Y"}},
                    ]
    cur_timestep = time_step
    cur_unit = time_unit
    cur_totalspan = total_span
    for itm in scale_params:
        _unit = list(itm.keys())[0]
        params = itm.get(_unit)
        _thd = params['thd']
        _period = params['period']
        _next_unit = params['next']
        if _next_unit=="W" and not use_week:
            continue
        if cur_unit==_unit and cur_timestep>_thd:
            cur_timestep = int(round(cur_timestep/_period))
            if cur_totalspan is not None:
                totalspan_value = int(round(cur_totalspan/np.timedelta64(_period,cur_unit)))
                cur_totalspan = np.timedelta64(totalspan_value,_next_unit)
            cur_unit = _next_unit

    """
        # {"D": {"thd": 7, "period": 60, "next": "W", "final_op": True}},
        # {"W": {"thd": 52, "period": 60,"next":"Y","final_op":False}},
    """
    # if readable_unit == "s" and time_step > 59:
    #     readable_unit = "m"
    #     period = 60
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))
    # if readable_unit == "m" and time_step > 59:
    #     readable_unit = "h"
    #     period = 60
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))
    # if readable_unit == "h" and time_step > 23:
    #     readable_unit = "D"
    #     period = 24
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))
    # if readable_unit == "D" and time_step > 27:
    #     readable_unit = "M"
    #     period = 30
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))
    # if readable_unit == "D" and time_step > 364:
    #     readable_unit = "Y"
    #     period = 365
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))
    # if use_week and readable_unit == "D" and time_step > 6:
    #     readable_unit = "Y"
    #     period = 7
    #     time_step = int(round(time_step / period))
    #     if total_span is not None:
    #         total_span = int(round(total_span / period))

    if total_span is None:
        return cur_timestep,cur_unit
    else:
        return cur_timestep,cur_unit,cur_totalspan


def convert_influxtime_to_numpy(time_str):
    _mapper = {'d':'D','w':'W','h':'h','m':'m','s':'s'}
    return _mapper.get(time_str)

def convert_influxtime_to_pandas(time_str):
    _mapper = {'d': 'D', 'w': 'W', 'h': 'H', 'm': 'min', 's': 'S'}
    return _mapper.get(time_str)

def get_unit_index(time_unit):
    """ 获取时间单位的下标，下标越大单位越大

    :param time_unit: 时间单位可读名称
    :return: 返回时间单位下标，越大则单位越大
    """
    _units = ["ns","us","ms",'s',"m","h","D","W","M","Y"]
    if time_unit in _units:
        return _units.index(time_unit)
    else:
        raise Exception("time unit not recognized !! ")

# 自适应获取训练数据的周期规律类型
def get_train_seasonly(timeseries_dates,time_step=None):
    """ 基于时间序列的长度决定在训练时是否增加季度/周/天的周期性规律，以及数据最小单位，
        若数据最小单位为min则周期的单位就需要为min，一天的周期值就是24*60；
        若单位为h则周期值就是24，因此需要具体判断

    :param timeseries_dates: 待判定时间序列长度，np.datetime64类型
    :param time_step: 时间步长，若为None则基于timeseries_dates自动推导=（末日-首日）/时间单位/时间列表长度
    :return: 训练时的周期类型以及每个周期相对于单位长度的数量,
            key:value形式，key为周期类型，value为[period_size,period_step]
    """
    result_period_dict = {"Y": [], "M": [], "W": [], "D": [],
                          "h": []}  # 只考虑年度/月度/周/天/小时级别周期，数值为[period_size,period_step]

    ##------------------------------------------
    try:
        _time_step,timeseries_unit,total_span = get_timeseries_stepunit(timeseries_dates)
    except Exception as e:
        return result_period_dict

    if time_step is None: #步长单位数量
        time_step = _time_step

    ##------------------------------------------
    # 默认图片能显示60个单位，3个单位一个label能显示20个label unit:(timedelta,showspan)
    DateUnitDict = collections.OrderedDict({"Y": {"threshold":np.timedelta64(25, "M"),
                                                  "period":np.timedelta64(1,"M"),
                                                  "period_size" : 12,
                                                  "period_unit": 'M',
                                                  },  # 25个月以上，可计算年度规律，年度规律为12个月一个周期
                                            # TODO 月周期不相等，数据容易对不齐
                                            # "M": {"threshold":np.timedelta64(2, "M"),
                                            #       "period":np.timedelta64(1,"M"),
                                            #       "period_size" : 1,
                                            #       "period_unit": 'M',
                                            #       },  # 2个月以上，可计算月度规律，
                                            "W": {"threshold": np.timedelta64(2, "W"),
                                                  "period": np.timedelta64(1, "D"),
                                                  "period_size": 7,
                                                  "period_unit": 'D',
                                                  },  # 2周以上，可计算周度规律
                                            "D": {"threshold":np.timedelta64(50, "h"),
                                                  "period":np.timedelta64(1,"h"),
                                                  "period_size" : 24,
                                                  "period_unit": 'h',
                                                  },  # 50h以上，可计算天级别规律
                                            "h": {"threshold":np.timedelta64(150, "m"),
                                                  "period":np.timedelta64(1,"m"),
                                                  "period_size" : 60,
                                                  "period_unit": 'm',
                                                  },  # 150min以上可计算小时级别规律
                                            })

    for unit_key in DateUnitDict:
        _thd_prd = DateUnitDict.get(unit_key)
        _threshold = _thd_prd.get("threshold").astype(total_span.dtype)
        _period = _thd_prd.get("period").astype(total_span.dtype)
        if np.timedelta64(time_step,timeseries_unit).astype(total_span.dtype) > _period:
            # 不考虑比时间单位更小的周期问题
            break

        _period_size = _thd_prd.get("period_size")
        if total_span > _threshold:
            period_step = _period / np.timedelta64(time_step, timeseries_unit)
            result_period_dict[unit_key]=[_period_size,period_step]  #[period_size,period_step]
            print("added seasonal var:  {}  with period_size {} and period_step {}".format(get_period_name(unit_key),_period_size,period_step))

    return result_period_dict


###====================================================
###                  测试函数区
###====================================================
def hello_resort_timeseries():
    import pandas as pd
    fname = ["co2_data","example_retail_sales","kpi_hello","localhost_disk"]
    for f in fname:
        data_file = os.path.abspath(basedir + "/data/univars/{}.csv".format(f))
        dataframe = pd.read_csv(data_file)
        if f=="kpi_hello":
            tc = "timestamp"
        else:
            tc = "ds"
        rlt = resort_timeseries(dataframe,time_as_index=True,timecolumn=tc)
        print(rlt)

def hello_autoscale_stepunit():
    test_case = {"s":[1,60,3600,86400,2505600,31536000],
                 "m":[1,60,1440,41760,525600],
                 "h":[1,24,696,8760],
                 "D":[1,7,30,365],
                 "M":[1,12]}
    for k in test_case:
        for s in test_case.get(k):
            print("{}-{} ==> ".format(s,k),autoscale_timestep_unit(s,k,
                                                                   # use_week=True
                                                                   ))


def run():
    # hello_resort_timeseries()
    # hello_autoscale_stepunit()
    # bjdate = get_datetime_beijing()
    # print(bjdate)
    print(convert_datestr_to_timestamp('2017-03-23 13:00:00'))


if __name__ == "__main__":
    run()
