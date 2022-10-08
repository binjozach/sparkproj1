package pack5

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object obj {
  
  def main(args:Array[String]):Unit={

  	val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			val csvdf = spark.read.format("csv").load("file:///C:/data/datatxns.txt")
csvdf.show()


val jsondf = spark.read.format("json").load("file:///C:/data/devices.csv")
jsondf.show()


val orcdf = spark.read.format("orc").load("file:///C:/data/data.orc")
orcdf.show()	

val pardf = spark.read.format("parquet").load("file:///C:/data/data.parquet")
pardf.show()

}
}