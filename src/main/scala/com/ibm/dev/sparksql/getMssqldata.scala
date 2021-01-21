package com.ibm.dev.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object getMssqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getMssqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //one way to get data from any db
    val qry = "(select * from abhidf where sal>2500) abc "
    //recommended for pyspark ...
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val df = spark.read.format("jdbc").option("user","msuername").option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable",qry).option("url",url).load()
   // df.show()

    //recommended for scala/java
    import java.util.Properties
    //second way to get data from db
    val msprop = new Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val msdf = spark.read.jdbc(url,qry,msprop)
    msdf.show()
    spark.stop()
  }
}
