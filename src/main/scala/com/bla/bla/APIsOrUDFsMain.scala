package com.bla.bla

import com.bla.bla.features.ClientsFeatures
import com.bla.bla.utils.Generators
import org.apache.spark.sql.functions._
import com.bla.bla.utils.SparkSession._

object APIsOrUDFsMain {

  def main(args: Array[String]): Unit = {

    val clientsDF = Generators.generateClientsDataFrame

    /**
      * The idea here is to transform the dataframe thanks to a function provided by the API
      * CF ASSOCIATED TESTS
      *
      * Pros:
      *   Probably more optimized
      *   Less serialization and network traffic
      *
      * Cons:
      *   A lot harder to test
      */

    val clientWithAdultTagAPI = ClientsFeatures.tagAdultClientsDF(clientsDF)

    /**
      * The idea here is to transform the dataframe thanks to a BUSINESS function injected in an UDF
      * CF ASSOCIATED TESTS
      *
      * Pros:
      *   A lot easier to test
      *   Able to deal with business functions all along and couple with Spark only when needed
      *
      */

    val clientWithAdultTagUDF = clientsDF.withColumn("isAdult", udf(ClientsFeatures.isAdult _).apply(col("age")))

    sc.stop()
  }
}
