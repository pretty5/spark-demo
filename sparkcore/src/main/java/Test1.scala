import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
       val conf =  new SparkConf()
    /* conf.setMaster("local[*]").setAppName("demo")*/
      val sc=  new SparkContext(conf)
   val rdd =  sc.textFile("hdfs://192.168.18.3:9000/spark/b.txt",4)
    val fRdd = rdd.flatMap { _.split(" ") }
    val mrdd = fRdd.map { (_, 1) }
    val rbkrdd = mrdd.reduceByKey(_+_)
    // 写入数据到hdfs系统
    rbkrdd.saveAsTextFile("hdfs://192.168.18.3:9000/wcresult")

  }
}
