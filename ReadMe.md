# TaosClient 支持ORM的客户端

## version 0.1
-[x] 继承并修改自：https://github.com/machine-w/crown.git  
-[x] 增加基于linux/windows本地驱动的连接方式(已测试)  
-[x] 将本地驱动连接中执行select时的底层接口由fetchall改为 fetchall_block,效率提升明显    
-[x] 数据拉取的耗时要小于数据转换的耗时，比如打印数据、转换dataframe等   
-[x] 对于拥有ts的数据结果，支持基于head和data转换为DataFrame结构   

## version 0.2
-[x] 在taos_client中增加基于yaml配置文件的启动方式   