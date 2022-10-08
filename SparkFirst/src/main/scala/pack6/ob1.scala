package pack6

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object ob1 {
  //df.select("Name", "Marks").show()
  
  
   def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			val usdf=spark.read.format("csv").option("header","true").
			        load("file:///C:/data/usdata.csv")
			        
			usdf.select("first_Name", "last_Name").show()
			
			 usdf.show()
  }
}