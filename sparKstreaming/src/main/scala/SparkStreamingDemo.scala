import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc=new StreamingContext(conf,Seconds(5))
    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)

    ds.map(word=>(word,1))
      .reduceByKey(_+_)
      .print()

    //启动streaming context
    ssc.start()
    ssc.awaitTermination()




  }
}
