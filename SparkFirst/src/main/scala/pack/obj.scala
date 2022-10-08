package pack



object obj {
  
  def main(arg:Array[String]):Unit={
   
    
    
val liststr = List("BigData-Spark-Hive",
                    "Spark-Hadoop-Hive")

liststr.foreach(println)
println("================flat List============")
println                
                      

			
val flatdata = liststr.flatMap( x => x.split("-"))
flatdata.foreach(println)











//val lisstr1 = List(  
//		"State->TN~City->Chennai"  ,     
//		"State->Gujarat~City->GandhiNagar"
//		)
//lisstr1.foreach(println)
//val flatmap1= lisstr1.flatMap( x => x.split("~"))
//flatmap1.foreach(println)
//val flatmap2= flatmap1.flatMap( x => x.split("->"))
 //   flatmap2.foreach(println)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    //println("zeyobron started")
    
    
    //println("===== raw list ======")
			
			//val liststr = List(
			    
			//		"Amazon-Jeff-America",
		//			"Microsoft-BillGates-America",
		//			"TCS-TATA-india",
		//			"Reliance-Ambani-INDIA"
					
		//			)
					
		//	liststr.foreach(println)
    
   // println("===== filter list ======")
			
	//		val filstr = liststr.filter( x => x.toLowerCase().contains("india"))
			
	//		filstr.foreach(println)
	//		println("=====flatten with Hyphen======")
			
	//		val flatdata = filstr.flatMap( x => x.split("-")) 
			
	//		flatdata.foreach(println)
			 
	//		println("=======replace data=====")
	//		val mapdata = flatdata.map( x => x.replace("india","local"))
	//		mapdata.foreach(println)
			 
	//		println("=======concat data=====")
	//		val concatdata = mapdata.map( x => x.concat(",Done"))		
	//		concatdata.foreach(println)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
   //  val liststr = List("zeyo~analytics",
   //                    "bigdata~spark",
  //                     "hive~spark")
    
  //  liststr.foreach(println)
    
    
  //   println("=====flattening====")
    
  // val flatdata = liststr.flatMap(x => x.split("~"))
    
  //  flatdata.foreach(println)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
   // val liststr = List("zeyobron","analytics","zeyo")
    
  //  liststr.foreach(println)
    
    
 //    println("=====string replace====")
    
 //  val repstr = liststr.map(x => x.replace("zeyo","tera"))
    
 //   repstr.foreach(println)
    
    
    
    
    
    
    
    
    
    
    
   // println("=====string map====")
    
  //  val mapstr = liststr.map(x => x.concat(" Bigdata"))
    
  //  mapstr.foreach(println)
    
    
    
    
    
    
    
    
    //println("=====string filter====")
    
    
   //val filstr = liststr.filter( x => x.contains("zeyo"))
   
   //filstr.foreach(println)
   
    
   
  }
}