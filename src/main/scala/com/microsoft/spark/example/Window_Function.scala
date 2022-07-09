package com.microsoft.spark.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Window_Function {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder().appName("Window").master("local[*]").getOrCreate()
    import spark.implicits._
    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
  val df = simpleData.toDF("employee_name", "department", "salary")
  //  df.show()
  val windowSpec = Window.partitionBy("department").orderBy($"salary".desc)
  val df2 =  df.withColumn("row_num",row_number().over(windowSpec))
  //val df3 = df2.select("employee_name","department","salary").where(col("row_num") === 1).show()

  //val df3 = df.withColumn("rnk",rank().over(windowSpec)).show()
  val df3 = df.withColumn("dns_rnk",dense_rank().over(windowSpec)).show()
  }

}
