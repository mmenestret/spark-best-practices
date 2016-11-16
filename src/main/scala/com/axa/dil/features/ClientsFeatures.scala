package com.axa.dil.features

import com.axa.dil.utils.Models.{Client, ClientWithAdultTag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by martin on 15/11/2016.
  */
object ClientsFeatures {

  /**
    * The idea is to discuss about the differences between DataFrames API and RDD API
    * Here we are using DataFrames
    *
    * Pros: We benefit from catalyst optimization (the filter probably append before the join)
    * Cons:
    * We have a DataFrame => DataFrame inexpressive and uncheckable at compile time
    * Catalyst makes stacks and execution flow a lot harder to troubleshout
    */

  def tagAdultClientsDF(clients: DataFrame): DataFrame = {
    clients.withColumn("isAdult", clients.col("age") >= 18)
  }

  def filterAdultClientsDF(clients: DataFrame): DataFrame = {
    clients.filter(clients.col("isAdult") === true)
  }

  /**
    * The idea is to discuss about the differences between DataFrames API and RDD API
    * Here we are using RDDs
    *
    * Pros:
    *   We benefit from type checking at compile time
    *   The dataflow is a lot easier to understand and to optimize
    * Cons:
    *   If the code isn't optimized and well organized catalyst won't magically help
    *   A LOT of strange, temporary case class are needed...
    */

  def tagAdultClientsRDD(clients: RDD[Client]): RDD[ClientWithAdultTag] = {
    clients.map {
      case Client(id, name, forname, age) => ClientWithAdultTag(id, name, forname, age, age > 18)
    }
  }

  def filterAdultClientsRDD(clients: RDD[ClientWithAdultTag]): RDD[Client] = {
    clients.filter(_.adultTag).map(c => Client(c.id, c.name, c.forname, c.age))
  }

  /**
    * The idea here is to transform the dataframe thanks to a BUSINESS function injected in an UDF
    * CF ASSOCIATED TESTS
    *
    * Pros:
    *   A lot easier to test
    *   Able to deal with business functions all along and couple with Spark only when needed
    */

  def isAdult(age: Int): Boolean = age >= 18

}
