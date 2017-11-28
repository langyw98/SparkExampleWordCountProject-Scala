import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kgc on 2017/11/28.
  */
object SimpleApp {
  def main(args: Array[String]){
    val logFile = "/home/hadoop/app/spark-1.6.3-bin-hadoop2.6/README.md"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile,2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs,numBs))
  }
}
