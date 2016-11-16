package com.bla.bla

import com.bla.bla.features.OrdersFeatures
import com.bla.bla.utils.{Generators, SparkSession}
import org.apache.spark.sql.DataFrame

object CodeOrganisationMain {

  def main(args: Array[String]): Unit = {

    val clientsDF = Generators.generateClientsDataFrame
    val ordersDF = Generators.generateOrdersDataFrame(clientsDF)

    /**
      * The idea is to compute several features at the same time and place for technical reasons
      * Here, the idea is to compute only one groupBy to produce 2 aggregations
      *
      * Pros: Only one groupBy
      * Cons: No idea what this function is about
      */

    val finalDF1 = OrdersFeatures.featuresGroupedByOrderId(ordersDF)


    /**
      * The idea is to separate the functional parts in different functions with less concern about technical stuff
      *
      * Pros: Functions are clearer
      * Cons: 1 groupBy and 1 join in each of them...
      */

    val ordersWithAvgPrices = OrdersFeatures.averagePrice(ordersDF)
    val finalDF2 = OrdersFeatures.averageNumberOfPieces(ordersDF)

    /**
      * The idea here is to fold over a list of "pure" function in order to materialize a data pipeline
      *
      * Pros: A lot clearer to read
      * Cons: It can mess up with column names during catalyst optimization phase (it's the case if you try to make it run)
      */

    List[DataFrame => DataFrame](
      OrdersFeatures.averagePrice,
      OrdersFeatures.averageNumberOfPieces,
      OrdersFeatures.otherDFtoDFfunction,
      OrdersFeatures.andAnotherDFtoDFfunction
    ).foldLeft(ordersDF){
      (acc, func) => func(acc)
    }

    sc.stop()
  }
}
