# bigdata-test

hadoop:

    hadoop hdfs操作

    log输出到flume

    flume输出到hdfs

hbase:

    HTable基本操作：创建，删除，添加表，行，列族，列等。

kafka：

    测试 producer | consumer

storm：实时处理消息


kafka集成storm集成hdfs

    读取kafka数据=》storm实时处理（分割字符，统计字符）=》写入hdfs


 * kafka消息生成方式：

 *   1. LogGenerator生成测试日志发送到flume=》

 *   2. MyKafkaProducer发送测试消息


 * MyKafkaStormHdfs 实时处理消息：

 *   =》读取kafka数据

 *   =》storm实时处理（分割字符，统计字符）

 *   =》写入hdfs


