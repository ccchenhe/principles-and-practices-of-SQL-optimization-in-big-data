  /**
   * 合并小文件并写入hive表
   * @param df
   * @param tableName 表名
   * @param schema 表的schema，可选默认值
   * @param isPersist 是否对df进行缓存，如果在调用这个方法之前，已经缓存了，那么需传入false
   * @param isUnPersist  是否写入hive表后，立即释放缓存，如果后续代码中需要复用这个df，那么最好传入false（不立即释放）
   * @param saveMode 默认 overwrite
   * @param sinkFormat 默认 parquet
   */
def repartitionIntoTable(df: DataFrame,tableName: String,schema: String = schema,isPersist: Boolean = true,isUnPersist: Boolean = true,saveMode: String = "overwrite",sinkFormat:String = "parquet"):Unit = {
    if(isPersist){
      df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    // 统计数据量大小
    val dataSize = df.count()
    // 根据数据量计算重分配的文件数
    val targetPartitions = (dataSize / DEFAULTPARTITIONROWSIZE)+1

    logger.info(s"logg  写入数据条数：${dataSize},调整成${targetPartitions}个分区。目标表:${schema}.${tableName}")

    df
      .repartition(targetPartitions.toInt)
      .write.mode(saveMode).format(sinkFormat).insertInto(schema+"."+tableName)
    if(isUnPersist){ //写完数据是否马上释放缓存
      df.unpersist()
    }
  }


