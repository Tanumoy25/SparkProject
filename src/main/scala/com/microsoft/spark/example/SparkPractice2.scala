package com.microsoft.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPractice2 extends App{
  val spark=SparkSession.builder().appName("practice2").master("local[*]").getOrCreate
  import spark.implicits._
  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "value")
  input.createOrReplaceTempView(("input"))
  val finalDf = spark.sql("""with cte as
    (Select id,value,row_number() over (partition by id order by id asc) as rnk from input)
    Select id,value,rnk from cte where rnk=1""")

  finalDf.show()
}
