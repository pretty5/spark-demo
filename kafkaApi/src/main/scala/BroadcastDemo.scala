import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setMaster("local[*]").setAppName("as"))
    val broadcastVar = sc.broadcast(List("1 human","2 animal"))
    println(broadcastVar.value)
    sc.parallelize(List("1 jack 1","2 rose 1","3 tom 2","4 jerry 2"))
      .map(line=>{
        val arr=line.split(" ")
        val cid=arr(2)
        val className=broadcastVar.value.map(line=>line.split(" ")).filter(arr=>arr(0).equals(cid))
          .map(arr=>arr(1)).head
        (arr(0),arr(1),className)
      }).foreach(println)

    //stu
    //1 jack 1
    //2 rose 1
    //3 tom 2
    //4 jerry 2

    // class
    //1 human
    //2 animal

  }
}

