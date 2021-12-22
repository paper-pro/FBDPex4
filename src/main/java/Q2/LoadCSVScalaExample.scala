package Q2

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

object LoadCSVScalaExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MyLoadCSVScalaExampleApplication").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val result = input.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }//等价于 val result = input.map(_.split(","))
    val temp1 = result.map(x => x(2))
    val header = temp1.first
    val temp2 = temp1.filter(row=>row!=header)
    val res = temp2.map(_.toDouble/1000.0).map(x=>scala.math.round(scala.math.floor(x))*1000).map(x => ((x, x+1000),1)).reduceByKey(_+_).sortByKey()
    res.saveAsTextFile(args(1))
  }
}