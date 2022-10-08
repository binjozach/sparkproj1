package pack5
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object obj3 {
  
  
  def main(args:Array[String]):Unit={


			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._



		//	val xmldf = spark.read.format("xml")
		//	.option("rowTag","book")
		//	.load("file:///C:/data/book.xml")


	//		xmldf.show()
	//		xmldf.printSchema()

			
			
				
			
			
			
			
			
//val xmldf = spark.read.format("xml")
		//	.option("rowTag","POSLog")
		//	.load("file:///C:/data/transactions.xml")


		//	xmldf.show()
		//	xmldf.printSchema()
 
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
val df = spark.read.format("csv").load("file:///C:/data/datatxns.txt")

//df.write.format("csv").save("file:///C:/data/mdata")      

 //--- Run it

//=================================

//Run again the same Code
//=================================

//df.write.format("csv").mode("append").save("file:///C:/data/mdata")       



//=================================

//df.write.format("csv").mode("overwrite").save("file:///C:/data/mdata")         



//=================================
//wait for a Min

df.write.format("csv").mode("ignore").save("file:///C:/data/mdata")  

	}
}