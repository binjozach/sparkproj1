 

package pack5
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object obj6 {
  
  
  def main(args:Array[String]):Unit={


			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")


			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._



			val xmldf = spark.read.format("xml")
			.option("rowTag","book")
			.load("file:///C:/data/book1.xml")


			xmldf.show()
			xmldf.printSchema()

			
			
				
			
			
			
			
			
//val xmldf = spark.read.format("xml")
		//	.option("rowTag","POSLog")
		//	.load("file:///C:/data/transactions.xml")


		//	xmldf.show()
		//	xmldf.printSchema()
 
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
		 

	}
}