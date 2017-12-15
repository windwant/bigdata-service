# elasticjob

当当 elastic job 应用测试项目

分布式定时任务框架 elastic-job


基本配置应用


DataflowJob SimpleJob


<reg:zookeeper id="regCenter" server-lists="${serverLists}" namespace="${namespace}" base-sleep-time-milliseconds="${baseSleepTimeMilliseconds}" max-sleep-time-milliseconds="${maxSleepTimeMilliseconds}" max-retries="${maxRetries}" />


<job:simple id="springSimpleJob" class="${simple.class}" registry-center-ref="regCenter" sharding-total-count="${simple.shardingTotalCount}" cron="${simple.cron}" sharding-item-parameters="${simple.shardingItemParameters}" monitor-execution="${simple.monitorExecution}" failover="${simple.failover}" description="${simple.description}" disabled="${simple.disabled}" overwrite="${simple.overwrite}" event-trace-rdb-data-source="elasticJobLog" />

