package pack6

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
		
object obj2 {
  case class schema(
		txnno:Int,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String
		)
  
			


def main(args:Array[String]):Unit={


		println("================Started1============")



		val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		val spark = SparkSession.builder().getOrCreate()
		 
import spark.implicits._ 
import org.apache.spark.sql.functions._


val df1 = spark.read.format("csv")
                .option("header", "true")
                .option("delimiter", "~")
                .load("file:///c:/data/file1.csv")

   

  

df1.show()

val df2 = spark.read.format("csv")
                  .option("header", "true")
                  .option("delimiter", "~")
                  .load("file:///c:/data/file2.csv")

   

  

df2.show()
val exdf = df1.withColumn("empman", explode(split(col("empman"), ",")))

exdf.show()

 

 
 

val joinl = exdf.join(df2, exdf("empman")===df2("mno"), "inner")
                .drop("mno")
                

 

val finaldf = joinl.groupBy("empno", "empname")
                  .agg(collect_list("mname").as("mname"))
                  .withColumn("mname", concat_ws(",",col("mname")))


                  
finaldf.show()



















/* val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/reqres.json")
		              
		    df.printSchema()           
		val flatdf = df.withColumn("data", explode(col("data")))
		                .select(col("data.*"), 
		                    col("page"),
		                    col("per_page"),
		                    col("support.*"),
		                    col("total"),
		                    col("total_pages"))        
		              
		flatdf.show()
		flatdf.printSchema()

    val complexdf = flatdf.withColumn("support",
                                struct(
                                   col("text"),col("url") 
                                      )
                                ).drop("text","url")
                          .groupBy("page", "per_page","total","total_pages","support")
                          .agg(
                              collect_list(
                                  struct(
                                        col("avatar"), col("email"), col("first_name"), col("id"), col("last_name")
                                        
                                          )
                                          ).as("data")
                                          )
                                          
          complexdf.show()
		      complexdf.printSchema()



*/


/*
val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/cm1.json") 

df.show()
df.printSchema()

val flattendf  = df.withColumn("Students",explode(col("Students")))
    
 flattendf.show()
flattendf.printSchema()


val complexdf = flattendf.groupBy("Technology","TrainerName","id")
                          .agg(collect_list("students").as("Students"))

complexdf.show()
complexdf.printSchema()





*/











		/*	val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
      val urlstring = html.mkString
      println(urlstring)

		
		
		
		val rdd  = sc.parallelize(List(urlstring))
		val df = spark.read.json(rdd)
		val flattendf = df.select(
		                    col("nationality"),
		                    explode(col("results")).as("results"),
		                    col("seed"),
		                    col("version")
		                    )
		                    
		                    
	val finaldf = flattendf.select( 
	                               col("nationality"),
	                               col("results.user.cell"),
	                               col("results.user.dob"),
	                               col("results.user.email"),
	                               col("results.user.gender"),
	                               col("results.user.location.*"),
	                               col("results.user.md5"),
	                               col("results.user.name.*"),
	                               col("results.user.password"),
	                               col("results.user.phone"),
	                               col("results.user.picture.*"),
	                               col("results.user.registered"),
	                               col("results.user.sha1"),
	                               col("results.user.sha256"),
	                               col("results.user.username"),
	                               col("seed"),
		                             col("version")
	                               ) .withColumn("today",current_date)
	  flattendf.show()
	  flattendf.printSchema()
	                               
 finaldf.write.format("csv")
             .partitionBy("today")
             .mode("append")
             .save("file:///c:/data/urldata")
		
		
		
		*/
		
		
		
		
		
		
		
		
		
		
		
		
		
	/*	val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/pets.json")
		              
		  df.show()
		  df.printSchema()            
		
		  val flatfinal = df
		                    .withColumn("Pets", explode(col("Pets")))
                        .withColumn("Permanent address", expr("Address.`Permanent Address`"))
		  	                .withColumn("Current address", expr("Address.`current Address`"))  
		  	                .drop("Address")
			flatfinal.show()
		  flatfinal.printSchema()    
		  
		  
		  
		  */
		  
		  
		  
		  
		
		/*
		val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/reqres.json")
		              
		            
		              
		    	df.show()
		df.printSchema()
		val flatdf = df.select(
		                   explode(col("data")).as("data") ,
		                          col("page"),
		                          col("per_page") ,
		                          col("support.*"),
		                           col("total"),
		                           col("total_pages")
		                           )
		                           
			flatdf.show()
		  flatdf.printSchema()    
		  
		  val flatfinal = flatdf.select (
		                                col("data.*"),
		                                col("page"),
		                                col("per_page") ,
                          		      col("text"),
                          		      col("url"),
                          		      col("total"),
		                                 col("total_pages")
                            )
               
		  	                           
			flatfinal.show()
		  flatfinal.printSchema()    
		  */
		/*
		val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/pets.json")
		              
		            
		              
		    	df.show()
		df.printSchema()
		val flattendf = df.select(
		                          col("Address.*"),
		                          col("Name") ,
		                          col("Mobile"),
		                           col("status"),
		                           explode(col("pets")).as("Pets") 
		                          
		                           )
		                           
			flattendf.show()
		  flattendf.printSchema()          
		              
		         */     
		              
		              
		              
		              
		              
		/*
		val  df = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/donut.json")
		             
		df.show()
		df.printSchema()
		val flattendf = df.select(
		                          col("id"),
		                          col("image.height").as("image_height"),
		                           col("image.url").as("image_url"),
		                           col("image.width").as("image_width"),
		                           explode(col("image.sizes")),
		                           col("name"),
		                            col("thumbnail.height").as("thumbnail_height"),
		                           col("thumbnail.url").as("thumbnail_url"),
		                           col("thumbnail.width").as("thumbnail_width"),
		                           col("type")
		                           )
		                           
			flattendf.show()
		  flattendf.printSchema()
		
		val complexdf = flattendf.select(
		                              col("id"),
		                              struct(
		                                  col("image_height").as("height"),
		                                 col("image_url").as("url"),
		                                 col("image_width").as("width") 
		                             ).as("image"),
		                              col("name"),
		                              struct(
		                                   col("thumbnail_height").as("height"),
		                                 col("thumbnail_url").as("url"),
		                                 col("thumbnail_width").as("width") 
		                              
		                              ).as("thumbnail"),
		                                col("type")
		                           )
		                           
		       complexdf.show()
		       complexdf.printSchema()
		
		
		*/
		
		
	/*	val pldf = spark
		              .read
		              .option("header","true")
		              .option("delimiter", "~")
		              .format("csv")
		              .load("file:///C:/data/PractitionerLatest.txt")
		              .select(col("PROV_PH_DTL"))
	         
   val fndf = pldf.withColumn("tempCol", split(col("PROV_PH_DTL"), "\\$") (0))
                  .withColumn("PH TY_CD", split (col("tempCol"),"\\|")(0))
                  .withColumn("PH_NUM", split(col("tempCol"),"\\|")(1))
                  .withColumn("PH_EXT_NUM", split(col("tempCol"),"\\|")(2))
                  .withColumn("PH_PRIM_IND", split(col("tempCol"),"\\|")(3))
                  .drop(col("tempCol"))
                  .drop(col("PROV_PH_DTL"))
  fndf.show(false)

*/
/*val finaldf = findf.select("PY TY_CD", "PY_NUM", "PY_EXT_NUM", "PY_PRIM_IND")


finaldf.show(false)*/
		
		
	/*		val jsondf = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/reqapi.json")
		    
		   jsondf.show()
		   jsondf.printSchema()
		   
		   val flatdf = jsondf.select(
		                              	"data.*",
		                                 "page",
                                  	"per_page",
                                  	"support.*",
                                  	"total",
                                  	"total_pages"
                                  
                                  	
		                       )
		
		flatdf.show()
		flatdf.printSchema()
		
		*/
		
		
		
		
		
/*		val jsondf = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/cm.json")
		    
		   jsondf.show()
		   jsondf.printSchema()
		   
		   val flatdf = jsondf.select(
		                                 "id",
		                                 "address.*"                            
		                       )
		
		flatdf.show()
		flatdf.printSchema()
		*/
		
		
		
	/*	val csvdf = spark
		              .read
		              .option("header","true")
		              .format("csv")
		              .load("file:///C:/data/partdata.csv")
		    
		   csvdf.show()
		   csvdf.printSchema()
		   
		   val complexdf = csvdf.select(
		                                 col("Technology"),
		                                 col("TrainerName"),
		                                 col("id"),
		                                 struct(
		                                     struct(
		                                       col("permanent"),
		                                       col("temporary"),
		                                       col("workloc")
		                                       ).as("user")
		                                     ).as("address")
		                       )
		
		complexdf.show()
		complexdf.printSchema()
		*/
		
	/*	val jsondf = spark
		              .read
		              .option("multiline","true")
		              .format("json")
		              .load("file:///C:/data/picturem.json")
		    
		   jsondf.show()
		   jsondf.printSchema()
		   
		   val flatdf = jsondf.selectExpr(
		                                 "id",
		                                 "image.height as image_height",
		                                 "image.url as image_url",
		                                 "image.width as image_width",
		                                 "name",
		                                 "thumbnail.height as thumbnail_height",
		                                  "thumbnail.url as thumbnail_url",
		                                   "thumbnail.width as thumbnail_width",
		                                   "type"	                                   
		                       )
		
		flatdf.show()
		flatdf.printSchema()*/
		
		
		
		
		
		
		
		/*
		
		
		
		val collist = List( 
		                  "txnno" ,
		                  "txndate" ,
		                  "custno" ,
		                  "amount" ,
		                  "category" ,
		                  "product" ,
		                  "city" ,
		                  "state" ,
		                  "spendby"
		                  )
		
		
		
		
		
		
		
		
		
		
		
		
		

		val file1= sc
		.textFile("file:///C:/data/revdata/file1.txt")

		println
		println
		println("===Schema df====")
		println
		println
		println


		val gymdata = file1.filter( x => x.contains("Gymnastics"))

		val mapsplit = gymdata.map( x => x.split(","))

		val schemardd = mapsplit.map( x => schema(x(0).toInt,
				x(1),
				x(2),
				x(3),
				x(4),
				x(5),
				x(6),
				x(7),
				x(8))
				)

		val progym = schemardd.filter( x => x.product.contains("Gymnastics"))

		val schemadf = progym.toDF().select(collist.map(col): _*)

		schemadf.show(5)








		println
		println
		println("===Row df====")
		println
		println
		println







		val file2 = sc.textFile("file:///C:/data/revdata/file2.txt")

		val rowmapsplit = file2.map( x => x.split(","))

		val rowrdd = rowmapsplit.map( x => Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val schema1 = StructType(Array(
				StructField("txnno",IntegerType,true),
				StructField("txndate",StringType,true),
				StructField("custno",StringType,true),
				StructField("amount", StringType, true),
				StructField("category", StringType, true),
				StructField("product", StringType, true),
				StructField("city", StringType, true),
				StructField("state", StringType, true),
				StructField("spendby", StringType, true)
				))


		val rowdf = spark.createDataFrame(rowrdd, schema1).select(collist.map(col): _*)

		rowdf.show(5)







		
		
		
		
		
		
		
		
		
		
	  println
		println
		println("===csv df====")
		println
		println
		println
		
		
		
		val csvdf = spark
		            .read
		            .format("csv")
		            .option("header","true")
		            .load("file:///C:/data/revdata/file3.txt")
		            .select(collist.map(col): _*)
		
		
		csvdf.show(5)
		
		
		
		
		
		
		
		
		
		println
		println
		println("===json df====")
		println
		println
		println
		
		
		val jsondf = spark
		              .read
		              .format("json")
		              .load("file:///C:/data/revdata/file4.json")
		              .select(collist.map(col): _*)
		
		jsondf.show(5)
		
		
		
		
		println
		println
		println("===parquet df====")
		println
		println
		println
		
		
		val parquetdf = spark
		              .read
		              .load("file:///C:/data/revdata/file5.parquet")
		              .select(collist.map(col): _*)
		
		parquetdf.show(5)
		
		
		
		
		
		
	  println
		println
		println("===xml df====")
		println
		println
		println
		
		
		val xmldf = spark
		              .read
		              .format("xml")
		              .option("rowTag","txndata")
		              .load("file:///C:/data/revdata/file6")
		              .select(collist.map(col): _*)
		
		xmldf.show(5)
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		println
		println
		println("===union df====")
		println
		println
		println
		
		
		val uniondf = schemadf
		              .union(rowdf)
		              .union(csvdf)
		              .union(jsondf)
		              .union(parquetdf)
		              .union(xmldf)
		
		uniondf.show(5)
		
		



		
		
		
		
		
		
		println
		println
		println("===Proc df====")
		println
		println
		println
		
		
		
		val procdf = uniondf
		              .withColumn("txndate", expr("split(txndate,'-')[2]"))
		              .withColumnRenamed("txndate","year")
		              .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
		              .filter(col("txnno")>50000)
		              
		
		
		procdf.show(5)
		
		
		
		
		
		
		
		
		
		
		
		println
		println
		println("===cummulative df====")
		println
		println
		println
		
		
		
		val cummulativedf = procdf
		                  .groupBy("category")
		                  .agg(sum("amount").as("total"))
		
		
		cummulativedf.show()
		
		
		
		
		
		
		
		
		
				
		println
		println
		println("===write df====")
		println
		println
		println
		
		
		
		procdf.write.format("avro")
		      .mode("overwrite")
		      .partitionBy("category","spendby")
		      .save("file:///C:/data/revavrod")
		
		
		
		
		      
		      
		      
		      
		      
		println
		println
		println("===inner df====")
		println
		println
		println     
		      
		      
		val df1 = spark.read.format("csv").option("header","true")
		          .load("file:///C:/data/revdata/join1.csv")
		      
		val df2 = spark.read.format("csv").option("header","true")
		          .load("file:///C:/data/revdata/join2.csv")      
		      
		      
		val joindf = df1
		            .join(df2, df1("txnno")===df2("tno"),"inner")
		            .drop(df2("tno"))
		
		joindf.show()
		      
		      
		      
		     	      
		println
		println
		println("===left Anti df====")
		println
		println
		println    
		      
		      
		 val df3 = df2.select("tno")     
		      
		val antijoindf = df1
		            .join(df3, df1("txnno")===df3("tno"),"left_anti")
		            .drop(df3("tno"))
		
		antijoindf.show()
			
			
			*/
			
			
			
			
			
			
			
	 		/* val jsondf = spark
			          .read
			           .format("json")
			           .option("multiline","true")
			           .load("file:///C:/data/cm.json") 	
			  jsondf.show()
			  jsondf.printSchema()
			  
			  val flatdf = jsondf.select(
			                            "Technology",
			                            "TrainerName",
			                            "address.permanent",
			                            "address.temporary",
			                            "id"
			                )
			   flatdf.show()
			  flatdf.printSchema()
			
			 val dfUber = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/uber.csv") 
			   
 
			dfUber.withColumn("date",
    to_date(col("date"),"MM/dd/yyyy"))
    .withColumn("Day", date_format(col("date"), "E"))
    .groupBy("dispatching_base_number", "Day")
    .agg(sum("trips").as("TotalTrips"))
    .show()
			
    
    */
    
    
    
    
			// .withColumn("dayOfWeekStr", dayOfWeekStr(col("dayofweek")))
			
			
			/*
			
			 val df1 = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/join1.csv") 
			       df1.show()
			       val df2 = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/join2.csv") 
			      df2.show()
			      
			      
			       val df3=df2.select("tno")
			       df3.show()
			       
			       val joindf=df1.join(df2, df1("txnno") === df2("tno"),"left_anti").drop("tno")
			       joindf.show()   
			
			       
			       
			       
			       
			       
			       
			       
			       val full = df1.join(df2,df1("txnno")===df2("tno"),"full")
			          .withColumn("txnno",
			              expr("case when txnno is null then tno else txnno end")
			              )
			           .drop("tno")
			           .orderBy(col("txnno"))

			full.show()
			
			
			*/
			
			
			  /*     val df1 = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/join1.csv") 
			       df1.show()
			       val df2 = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/join2.csv") 
			      df2.show()
			      
			      
			       val joindf=df1.join(df2, df1("txnno") === df2("tno"),"left").drop("tno")
			       joindf.show()
			           
			           */
			           
			           
			           
			     
			   /*   val dtdf = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/usdata.csv")
			           
			     dtdf.show()
			     
			     val fdf = dtdf.filter( col("state") === "LA" 
			                            && 
			                            col("age") > 10
			                            )
			      fdf.show()
			      
			      
			      
			      
			     
			     val dtdf = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/dtdata.csv")
			           
			      dtdf.show()  
			  val windowDept = Window.partitionBy("dept").orderBy(col("salary").desc)
       val fdf= dtdf.withColumn("row",row_number.over(windowDept)).withColumnRenamed("salary", "SecondHighestSalary")
            .where($"row" === 2).drop("row")
        fdf.show()
  
	  */
			
			      
			    /*   val dtdf = spark
			          .read
			           .format("csv")
			           .option("header","true")
			           .load("file:///C:/data/dt.txt")
			           
			     dtdf.show()*/
			     
			  /*   val finaldf = dtdf
			                     .groupBy(col("category"))
			                     .agg(sum(col("amount")) as("total"))
			                     .orderBy(col("total").desc)
			     
			     finaldf.show()
			     
			         val finaldf1 = dtdf
			                     .groupBy(col("product"))
			                     .agg(sum(col("amount")) as("total"))
			                     .orderBy(col("product"))
			     
			     finaldf1.show()              
			    
			     
			     
			      val finaldf2 = dtdf
			                     .groupBy(col("category"))
			                     .agg(count(col("category")) as("Count"))
			                     .orderBy(col("category"))
			     
			     finaldf2.show()  */            
			     
			     
			     
			     
			     
			   /*  val finaldf = dtdf
			.withColumn("wdate",expr("split(tdate,'-')[2]"))
			//.withColumnRenamed("tdate", "year")
			.withColumn("status",expr("case when spendby='cash' then 0 else 1 end"))
			
		
			finaldf.show()
			
			*/
			
			
			
			
			
			
			
			
			
			  /*    val fdf = dtdf.selectExpr(
                            "id",
                            "split(tdate,'-')[2] as year",
                            "amount",
                            "category",
                            "product",
                            "spendby",
                            "case when spendby='cash' then 1 else 0 end as status"
                            )			
            fdf.show() 
			     
			     */
			     /*val fdf = dtdf.filter(
			                           col("category") === "Exercise"
			                           &&
			                           col("spendby") === "cash"
                              )
           fdf.show() 
           
           
           println("================Started1============")
			println
			 

		 	val dtdf = spark
			.read
			.format("csv")
			.option("header","true")
			.load("file:///C:/data/dt.txt")

			dtdf.show()
			println
			println("=========select================")
			println
			val sdf = dtdf.select("id","tdate")
			sdf.show()
			println
			println("=========filter================")
			println

			val fdf = dtdf.filter(

				 	 col("category")==="Gymnastics").filter(col("product") like "%Ring%")
				 	 

			fdf.show()
			println
			println("=========multiColumn and Filter================")
			println

			val adf = dtdf.filter(

					col("category")==="Gymnastics"
					&&
					col("spendby")==="cash"

					)

			adf.show()

			println
			println("=========or operator================")
			println
			val odf = dtdf.filter(

					col("category")==="Gymnastics"
					||
					col("spendby")==="cash"

					)

			odf.show()

			println
			println("=========multi value================")
			println

			val mdf = dtdf.filter(

					col("category") isin ("Gymnastics","Exercise")

					)

			mdf.show()

			println
			println("=========like operator================")
			println

			val cdf = dtdf.filter(

					// col("product") like "%Gymnastics%"
					    col("product") like "%Ring%"

					)

			cdf.show()

			println
			println("=========null filter================")
			println
			val ndf = dtdf.filter(

					col("id").isNull 

					)

			ndf.show()

			println
			println("=========not null filter================")
			println
			val nndf = dtdf.filter(

					!  (  col("id").isNull  )

					)

			nndf.show() 
*/
			
   }
}