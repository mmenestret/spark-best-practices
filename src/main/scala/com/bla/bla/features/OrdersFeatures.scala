package com.bla.bla.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by martin on 15/11/2016.
  */
object OrdersFeatures {

  def otherDFtoDFfunction(df: DataFrame): DataFrame = df
  def andAnotherDFtoDFfunction(df: DataFrame): DataFrame = df
  /**
    * The idea is to compute several features at the same time and place for technical reasons
    * Here, the idea is to compute only one groupBy to produce 2 aggregations
    *
    * Pros: Only one groupBy
    * Cons: No idea what this function is about
    */

  def featuresGroupedByOrderId(df: DataFrame): DataFrame = {
    df.groupBy(col("clientId"))
        .avg("price", "numberOfPieces")
  }

  // add an helper method to rename aggregated cols
  def renamed(df: DataFrame) : DataFrame = {
    val AVG = "avg\\((.*)\\)".r
    df.columns.foldLeft(df)((acc, name) => name match {
      case AVG(c) => acc.withColumnRenamed(name, s"avg$c")
      case _ => acc }
    )
  }

  /**
    * The idea is to separate the functionnal parts in different functions with less concern about technical stuff
    *
    * Pros: Functions are clearer
    * Cons: 1 groupBy and 1 join in each of them...
    */

  def averagePrice(df: DataFrame): DataFrame = {
    val dfWithAvgPrice = df.groupBy(col("clientId")).agg(avg("price").as("averagePrice"))
    df.join(dfWithAvgPrice)
  }

  def averageNumberOfPieces(df: DataFrame): DataFrame = {
    val dfWithAvgNumberOfPieces = df.groupBy(col("clientId")).agg(avg("numberOfPieces").as("averageNbOfPieces"))
    df.join(dfWithAvgNumberOfPieces)
  }
}
