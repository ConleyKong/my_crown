# TaosClient 支持ORM的客户端

## version 0.1
-[x] 继承并修改自：https://github.com/machine-w/crown.git  
-[x] 增加基于linux/windows本地驱动的连接方式(已测试)  
-[x] 将本地驱动连接中执行select时的底层接口由fetchall改为 fetchall_block,效率提升明显    
-[x] 数据拉取的耗时要小于数据转换的耗时，比如打印数据、转换dataframe等   
-[x] 对于拥有ts的数据结果，支持基于head和data转换为DataFrame结构   

## version 0.2
-[x] 在taos_client中增加基于yaml配置文件的启动方式   
-[x] 在example中增加基于模型定义的方式创建和管理表的能力    
-[x] 为cursors中的cursor抽象类增加__str__方法，提供直接print的能力  
-[x] 解决cursor继承自list后的无法索引问题，当前的cursor操作能够实现：
    1. 直接print（仿照dataframe的输出）；
    2. 直接下标索引访问元素；
    3. 直接访问data或者head属性获取结果信息；  
-[x] 完善crown源码中Model以及SuperModel做 describe 操作时，若表不存在则自动创建的能力      
-[x] 完善cursor中的to_dataframe过程，利用cursor本身是list的优势快速构建  
-[x] 完善to_dataframe中的主键索引设置问题，当ts不存在时将以head中的第一个为主键  
-[x] 增加cursor中的to_ndarray 过程，利用cursor本身是list的优势快速转换   
-[x] 更新了example中的对于limit/interval/group等用法的使用实例   
-[x] 优化工程结构，将必须的依赖代码放入my_crown内  