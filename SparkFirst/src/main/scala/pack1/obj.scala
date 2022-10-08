package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object obj {
  
  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
    
    val data =sc.textFile("file:///C:/data/usdata.csv")
    data.take(5).foreach(println)
    println
    println("==========length data===========")
    
    println
    
    
    val lendata = data.filter( x => x.length()>200)
		lendata.foreach(println)
    
    println
    println("==========flatten data===========")
    println
    
    
    val flatdata = lendata.flatMap( x => x.split(","))
		flatdata.foreach(println)
    
		println
    println("==========replace data===========")
    println
    
    
    val replacedata = flatdata.map( x => x.replace("-",""))
		replacedata.foreach(println)
		
		println
    println("==========replace data===========")
    println
    
    
    val concatdata = replacedata.map( x => x.concat(",zeyo"))
		concatdata.foreach(println)
		
		println("==========Data Write===============")
		
		concatdata.coalesce(1).saveAsTextFile("file:///C:/data/33dir")
		
    
		
		//	println("================Started============")
		//	println

		//	val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		//	val sc = new SparkContext(conf)
		//	sc.setLogLevel("ERROR")
		//	println("======== file data=====")

		//	println("======== file data=====")

		//	val liststr = sc.textFile("file:///C:/data/bdata.txt",1)

		//	liststr.foreach(println)
		//	println
		//	println("================flat List============")
		//	println                



		//	val flatdata = liststr.flatMap( x => x.split("-"))
		//	flatdata.foreach(println)


  }
  
}