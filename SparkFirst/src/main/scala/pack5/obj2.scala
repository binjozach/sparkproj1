package pack5


import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object obj2 {
  
  def main(args:Array[String]):Unit={


			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._



			val avrodf = spark.read.format("avro")

			.load("file:///C:/data/data.avro")


			avrodf.show()






	}
}