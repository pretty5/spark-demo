object SparkSqlJDBCSource {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "teacher2")
      .option("user", "root")

      .option("password", "123456")
      .load()


    /*jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "teacher2")
      .option("user", "root")
      .option("password", "123456")
      .save()*/



    jdbcDF.show()

  }

}
