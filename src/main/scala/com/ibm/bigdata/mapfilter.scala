package com.ibm.bigdata

import org.apache.spark.sql._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mapfilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val nums = Array(1, 4, 5, 3, 2, 9, 1)
    val nrdd = sc.parallelize(nums)
    // val res = nrdd.map(x=>x*x).filter(x=>x<20)
    //  res.collect.foreach(println)

    val data = "C:\\BigData\\Datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    //select * from tab where city=...
    //select city, count(*) from tab group by city
    //select join two tables
    // val res = aslrdd.filter(x=>x.contains("blr"))
    val skip = aslrdd.first()

    val res = aslrdd.filter(x => x != skip).map(x => x.split(",")).map(x => (x(0), x(1).toInt, x(2), x(3))).filter(x => x._3 == "blr" && x._2 < 30)
    //array(jyo,12,blr,fri)
    //array(koti,29,blr,saturday)
    res.collect.foreach(println)
    spark.stop()
  }
}
