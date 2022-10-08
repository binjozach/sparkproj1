package pack3

import org.apache.spark._

import org.apache.spark.sql.SparkSession

object obj {


case class schema(id:String,tdate:String,category:String,product:String)

def main(args:Array[String]):Unit={


		println("================Started============")
		println
		val conf = new SparkConf().setAppName("ES").setMaster("local[*]")


		val sc = new SparkContext(conf)
		
		val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

		sc.setLogLevel("ERROR")
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

		val schemardd = mapsplit.map( x => schema(x(0),x(1),x(2),x(3)))

		val filterrdd = schemardd.filter(  x => x.product.contains("Gymnastics") )


 
		val df = filterrdd.toDF()
    df.show()












}

}