#-*- coding:utf-8 -*-
"""
-----------------------------------
    Project Name:    myCrown
    File Name   :    __init__.py
    Author      :    Administrator
    Create Date :    2021/2/25
    Description :    关于本文件的描述
--------------------------------------------
    Change Activity: 
        2021/2/25 :    创建并初始化本文件
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


def run():
    pass


if __name__ == "__main__":
    run()       