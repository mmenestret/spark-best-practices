package com.axa.dil.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by martin on 15/11/2016.
  */
object SparkSession {

  val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val sqlContext = new SQLContext(sc)

}
