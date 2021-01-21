package com.ibm.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object sparkudf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\BigData\\Datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",";").load(data)
    //df.show()
    df.createOrReplaceTempView("tab")
    // val res = spark.sql("select * from tab where balance>70000 and marital='married'")
    //val res = df.where($"balance">=60000 && $"marital"==="married") //domain specific language
    //val res = spark.sql("select *, concat(job,'',marital,'',education) fullname, concat_ws(' ',job,marital,education) fullname1 from tab")
    val res = df.withColumn("fullname", concat_ws("_",$"marital",$"job",$"education"))
      .withColumn("fullname1",concat($"marital", lit("-"),$"job", lit("_"),$"education"))
    res.show(false)


    spark.stop()
  }
}
