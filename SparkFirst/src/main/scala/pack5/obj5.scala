package pack5

object obj5 {
  def main(args:Array[String]):Unit={
    
    
    val age=200
    
    age match {
      case 10 => println(age)
      case 20 => println(age)
      case 30 => println(age)
      case _ => println("def")
    }
    val num=2
    
    num match {
      case 1|3|5|7|9 => println("odd")
       case 2|4|6|8 => println("even")
    }
    
  val result =  num match {
      case 1|3|5|7|9 => "odd"
      case 2|4|6|8 => "even"   
        }
    println(result)
    var i=1
    println("Printing frm   WHILE")
    while(i<10)
    {
    println("i is " + i)
     i += 1;
    }
    
     println("Printing frm DO WHILE")
    do
    {
      println("i is " + i)
     i += 1;
    }while(i<10)
      
      
      
      for(i <- 1.to(5)) // for loop upto vals in bracket
      {
        println("i is " + i)
      }
    
    for(i <- 1.until(5)) // for loop until
      {
        println("i is " + i)
      }
      for(i <- 1 to 5; j <- 1 to 3) // for loop nested loop
      {
        println("i is " + i + " " + j)
      }
      
      val lst= List(1,2,3,4,5)
      for(i <- lst) // for loop upto vals in list
      {
        println(i)
      }
      for(i <- lst; if i <3) // for loop upto vals in list with condition 
      {
        println(i)
      }
      val res = for{i <- lst; if i <5} yield { //store valeus after evalution and exceution to list if mutiple values
        i*i
      }
      println(res)
      
      
    if( i==1)
    {
    
    
    val name = "Binu"
    val age = 24.2
    println(name + " is " + age + " years old")
    println(s"$name is $age years old")
    println(f"$name%s is $age%f years old")
    println(s"Hello \nworld")
    println(raw"Hello \nworld")
    
    /*Comment example
     * mutiline
     */
    //comment single line example 
    
    //conditions
    if(age>20) 
    {println(s"$name's age is greater than 20")
    }
    //single line if and result
    val res = if(age>20) "age is greater than 20" else "age less than 20"
      println(res)
    }
  }
}