package pack2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj {
  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
    
    val data =sc.textFile("file:///C:/data/txns")
    println
    println("Filter data with Gymnastics")
    println("----------------------------")
    val filterdata = data.filter( x => x.contains("Gymnastics"))
		filterdata.foreach(println)
  }
  
  
}