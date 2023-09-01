package com.microsoft.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPractice extends App {
  val spark = SparkSession.builder().appName("practice").master("local[*]").getOrCreate()
  import spark.implicits._
  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
 // dept.show()
  dept.createOrReplaceTempView("dept")
  val udf_delimit = udf{(x:String,y:String)=>x.split(y)}
  spark.udf.register("udf_split",udf_delimit)
  val finaldf= spark.sql("""Select *,udf_split(VALUES,Delimiter) as splitted from dept""")
  finaldf.show()

}
