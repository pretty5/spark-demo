object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df=spark.read
      .json("D:\\developenv\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")


    //rdd[String]
    //dataframe=rdd[Row]
    //dataset=rdd[Person]

   // df.filter(row=>row.getAs[Int]("age")>18)



    df.createOrReplaceTempView("p")

    spark.sql("select name from p where age > 18").show()

    //df.show()




  }

}
