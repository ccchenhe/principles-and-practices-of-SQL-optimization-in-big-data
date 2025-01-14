import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.ContentSummary
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
  * 根据文件夹内文件大小计算应该生成的文件数，之后再执行repartitionIntoTable方法
  */
def getFileNeedRepartitionNum(tableFileABSLocation: String, mergeSize: Int, url: String = "hdfs://"): Int ={
  val hdfs: FileSystem = FileSystem.get(new URI(url), new Configuration())
  val path: Path = new Path(tableFileABSLocation)
  // 获取hdfs文件大小、文件数量等
  val result = hdfs.getContentSummary(path)
  // 从字节数转换为MB
  val inputPathFileSize: Long = result.getLength / 1024 / 1024
  // 计算重分配的文件数
  val targetPartitionsNum: Int = scala.math.floor(inputPathFileSize / mergeSize + 1).toInt
  targetPartitionsNum
}