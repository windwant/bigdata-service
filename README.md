# hadoop-test

hadoop:

    hadoop hdfs操作

    log输出到flume

    flume输出到hdfs

hbase:

    HTable基本操作：创建，删除，添加表，行，列族，列等。

kafka：测试 producer | consumer

    Kafka是一种分布式的，基于发布/订阅的消息系统。主要设计目标如下：

    通过O(1)的磁盘数据结构提供消息的持久化，这种结构对于即使数以TB的消息存储也能够保持长时间的稳定性能。

    高吞吐量：即使是非常普通的硬件kafka也可以支持每秒数十万的消息。

    Consumer客户端pull，随机读,利用sendfile系统调用进行zero-copy ,批量拉数据

    消费状态保存在客户端

    支持Kafka Server间的消息分区，及分布式消费，同时保证每个Partition内的消息顺序传输。

    数据迁移、扩容对用户透明

    支持Hadoop并行数据加载。

    支持online(在线)和offline(离线)的场景。

    持久化：通过将数据持久化到硬盘以及replication防止数据丢失。

    scale out：无需停机即可扩展机器。

    定期删除机制，支持设定partitions的segment file保留时间。


    启动：> bin/zookeeper-server-start.sh config/zookeeper.properties

    创建topic：>bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

    查询topic：> bin/kafka-topics.sh --list --zookeeper localhost:2181

storm：kafka集成storm集成hdfs

读取kafka数据=》storm实时处理（分割字符，统计字符）=》写入hdfs


