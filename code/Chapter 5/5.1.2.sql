-- Hive中开启预聚合

-- 是否开启Map端聚合，默认为TURE
SET hive.map.aggr=TRUE;
-- 在Map端进行聚合操作的条数
SET hive.groupby.mapaggr.checkinterval=100000;
-- 发生数据倾斜时,进行负载均衡，配置时需要注意，这样虽然可以解决数据倾斜的问题。但是不能让运行速度更快，在数据量小的时候，开启该配置反而有可能导致任务执行时长变长
SET hive.groupby.skewindata=TRUE;

-- Flink中开启预聚合
-- 启用mini batch
SET table.exec.mini-batch.enabled=TRUE;
-- 批量输出数据的时间间隔
SET table.exec.mini-batch.allow-latency=5s;
-- 批量输出数据的最大记录数
SET table.exec.mini-batch.size=5000;
-- 聚合策略，AUTO、TWO_PHASE(使用LocalGlobal两阶段聚合)、ONE_PHASE(仅使用Global一阶段聚合)。
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
