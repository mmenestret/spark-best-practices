package com.bla.bla

import Models.{Client, ClientWithAdultTag}
import com.bla.bla.features.ClientsFeatures
import com.bla.bla.utils.{Generators, Models, SparkSession}
import com.bla.bla.utils.Models.{Client, ClientWithAdultTag}
import org.apache.spark.rdd.RDD

object DataFrameOrRDDMain {

  def main(args: Array[String]): Unit = {

    val clientsDF = Generators.generateClientsDataFrame
    val ordersDF = Generators.generateOrdersDataFrame(clientsDF)

    val clientsRDD = Generators.generateClientsRDD
    val ordersRDD = Generators.generateOrdersRDD(clientsRDD)

    /**
      * The idea is to discuss about the differences between DataFrames API and RDD API
      * Here we are using DataFrames
      *
      * Pros: We benefit from catalyst optimization (the filter probably append before the join)
      * Cons:
      * We have a DataFrame => DataFrame inexpressive and uncheckable at compile time
      * Catalyst makes stacks and execution flow a lot harder to troubleshout
      */

    val clientsWithAdultTagDF = ClientsFeatures.tagAdultClientsDF(clientsDF)
    val adultClientsDF = ClientsFeatures.filterAdultClientsDF(clientsWithAdultTagDF)

    /**
      * The idea is to discuss about the differences between DataFrames API and RDD API
      * Here we are using RDDs
      *
      * Pros:
      * We benefit from type checking at compile time
      * The dataflow is a lot easier to understand and to optimize
      * Cons:
      * If the code isn't optimized and well organized catalyst won't magically help
      * A LOT of strange, temporary case class are needed...
      */

    val clientsWithAdultTagRDD: RDD[ClientWithAdultTag] = ClientsFeatures.tagAdultClientsRDD(clientsRDD)

    //  ClientsFeatures.filterAdultClientsRDD(clientsRDD)
    // /!\ WOULDN'T COMPILE THANKS TO TYPE CHECKING (clientsRDD: RDD[Client] whereas filterAdultClientsRDD exepect RDD[ClientWithAdultTag] /!\
    val adultClientsRDD: RDD[Client] = ClientsFeatures.filterAdultClientsRDD(clientsWithAdultTagRDD)

    sc.stop()
  }
}
