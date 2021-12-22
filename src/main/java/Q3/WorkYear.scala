package Q3

import org.apache.spark.sql.SparkSession

object WorkYear {
  def main(args: Array[String]): Unit ={
    val spark : SparkSession = SparkSession.builder()
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    val pathIn = args(0)
    val df = spark.read.option("header", "true").csv(pathIn)
    df.createOrReplaceTempView("df")
    val temp = spark.sql("select user_id,censor_status,work_year from df where work_year is not null")
    temp.createOrReplaceTempView("temp")
    val temp2 = spark.sql("select user_id,censor_status,regexp_extract(work_year,'[0-9]+',0) work_year_No from temp")
    temp2.createOrReplaceTempView("temp2")
    val res = spark.sql("select user_id,censor_status,if(work_year_No=10,concat(work_year_No,'+'),work_year_No) work_year from temp2 where work_year_No>5")
    res.write.csv(args(1))
  }

}
