package pack4


import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object obj1 {
  
  def main(args:Array[String]):Unit={


			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._





			println
			val data = sc.textFile("file:///C:/data/datatxns.txt",1) // Your File path
			data.foreach(println)
			println
			println("========Gymnastics=======")

			println

			val gymdata = data.filter( x => x.contains("Gymnastics"))
			gymdata.foreach(println)


			println
			println("========Column 4th Gymnastics=======")

			println



			val mapsplit = data.map( x => x.split(","))
      
 			
			val rowrdd = mapsplit.map( x => Row( x(0),x(1),x(2),x(3)))
       
			val filterdata = rowrdd.filter( x => x(3).toString().contains("Gymnastics"))


			filterdata.foreach(println)

			val structSchema = StructType(Array(
					StructField("id", StringType, true),
					StructField("tdate", StringType, true),
					StructField("category", StringType, true),
					StructField("product", StringType, true)));  


     val df = spark.createDataFrame(filterdata, structSchema)
     
     df.show()

     
     df.createOrReplaceTempView("baahubali")
     
     
     val finaldf = spark.sql("select * from baahubali where tdate='06-26-2011'")
     
     finaldf.show()
   

  }
  
}