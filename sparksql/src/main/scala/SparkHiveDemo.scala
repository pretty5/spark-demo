import org.apache.spark.sql.SparkSession

object SparkHiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.18.3:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    // Queries are expressed in HiveQL
    sql("SELECT * FROM class.t_p").show()
  }
}
