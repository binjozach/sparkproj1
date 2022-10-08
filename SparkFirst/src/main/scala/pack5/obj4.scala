package pack5

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object obj4 {
  
  def main(args:Array[String]):Unit={


			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
  
  
  val df=   spark.time(spark.read.format("jdbc")
.option("url","jdbc:mysql://zeyodb.czvjr3tbbrsb.ap-south-1.rds.amazonaws.com/zeyodb")
.option("user","root")
.option("password","Aditya908")
.option("dbtable","kgf")
.option("driver","com.mysql.cj.jdbc.Driver")
.load())
df.show()
  }
  
}