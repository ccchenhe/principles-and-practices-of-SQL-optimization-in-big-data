// bucket join 实现方式的伪代码

def join(leftRDD: RDD[(String, String)], rightRDD: RDD[(String, String)]) {
  val joinedRDD: RDD[(String, Option[String], Option[String])] = leftRDD
    .zipPartitions(rightRDD) { (leftIter, rightIter) =>
      // 将rightIter的所有元素转换为一个Map
      val rightMemMap = rightIter.toMap
      // 对于leftIter中的每个元素，尝试从rightMemMap中寻找相同k的值
      leftIter.map { case (k, v) => (k, Some(v), rightMemMap.get(k)) }
    }
}
