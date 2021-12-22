package Q3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, sum}

object EmployerType {
  def main(args: Array[String]): Unit ={
    val spark : SparkSession = SparkSession.builder()
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    val pathIn = args(0)
    val df = spark.read.option("header", "true").csv(pathIn)
    val res = df.groupBy("employer_type").count().withColumn("fraction",col("count")/sum("count").over()).sort(desc("fraction")).drop("count")
    res.write.csv(args(1))
  }
}