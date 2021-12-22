package Q3

import org.apache.spark.sql.SparkSession

object TotalMoney {
  def main(args: Array[String]): Unit ={
    val spark : SparkSession = SparkSession.builder()
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    val pathIn = args(0)
    val df = spark.read.option("header", "true").csv(pathIn)
    df.createOrReplaceTempView("df")
    val sqlDF = spark.sql("select user_id, year_of_loan*monthly_payment*12-total_loan as total_money from df")
    sqlDF.write.csv(args(1))
  }
}

