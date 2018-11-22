import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
       val conf =  new SparkConf()
        conf.setMaster("local[*]").setAppName("demo")
      val sc=  new SparkContext(conf)
   val rdd =  sc.textFile("E:\\test\\123.txt",4)
    rdd.groupBy(line=>line.split("\t")(0)).collect().foreach(println)
  }
}
