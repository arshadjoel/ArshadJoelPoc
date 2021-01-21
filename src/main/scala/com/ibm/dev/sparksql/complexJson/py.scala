package com.ibm.dev.sparksql.complexJson

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object py {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("py").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}
