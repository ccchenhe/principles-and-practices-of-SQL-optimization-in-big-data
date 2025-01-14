-- Hive 的小文件合并参数
//每个Map最大输入大小(这个值决定了合并后文件的数量)
SET mapred.max.split.size=256000000;  
//一个节点上split的至少的大小(这个值决定了多个DataNode上的文件是否需要合并)
SET mapred.min.split.size.per.node=100000000;
//一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)  
SET mapred.min.split.size.per.rack=100000000;
//执行Map前进行小文件合并
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
//设置Map端输出进行合并，默认为TRUE
SET hive.merge.mapfiles=TRUE;
//设置Reduce端输出进行合并，默认为FALSE
SET hive.merge.mapredfiles=TRUE;
//设置合并文件的大小
SET hive.merge.size.per.task=256*1000*1000;
//当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge。
SET hive.merge.smallfiles.avgsize=16000000;


-- Spark SQL的REPARTITION合并
INSERT OVERWRITE TABLE feed_item PARTITION (partition_date="xx")
SELECT /*+REPARTITION(6)*/ t.item_id,
                           t.shop_id,
                           t.feed_id,
                           t.uid,
                           t.content_type
FROM (SELECT t2.items['item_id'] AS item_id,
-- ...

-- Spark SQL的COALESCE合并
INSERT OVERWRITE TABLE feed_item PARTITION (partition_date="xx")
SELECT /*+COALESCE(6)*/ t.item_id,
-- ...
