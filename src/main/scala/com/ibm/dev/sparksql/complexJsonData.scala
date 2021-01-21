package com.ibm.dev.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexJsonData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexJsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "file:///c:\\BigData\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
    df.createOrReplaceGlobalTempView("tab")
    df.printSchema()
    val qry = "select_id id, city, loc[0] lang, loc[1] lati, pop"
    val res = spark.sql(qry)
    res.printSchema()
    res.show(6)
    spark.stop()
    df.show()

    spark.stop()
  }
}
