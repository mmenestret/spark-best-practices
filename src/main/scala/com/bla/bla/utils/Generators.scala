package com.bla.bla.utils

import Models.{Client, Order}
import org.apache.spark.rdd.RDD
import SparkSession._
import com.bla.bla.utils.Models.{Client, Order}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

/**
  * Created by martin on 15/11/2016.
  */
object Generators {

  val nbOfClients = 2000
  val nbOfOrders = 10000

  val maxAge = 100
  val maxNbOfPieces = 5
  val maxPrice = 10000

  def generateClients: Seq[Client] = {
    (0 to nbOfClients).map {
      i => Client(i, s"Name$i", s"Forname$i", Random.nextInt(1 + maxAge))
    }
  }

  def generateOrders(clientList: Seq[Client]): Seq[Order] = {
    val clientIdList = clientList.map(_.id)
    (0 to nbOfOrders).map {
      i => Order(
        id = i,
        clientId = clientIdList(Random.nextInt(clientIdList.length)),
        numberOfPieces = Random.nextInt(1 + maxNbOfPieces),
        price = Random.nextDouble() * 1000 % maxPrice)
    }
  }

  def generateClientsRDD: RDD[Client] = sc.parallelize(generateClients)

  def generateOrdersRDD(clientsRDD: RDD[Client]): RDD[Order] = sc.parallelize(generateOrders(clientsRDD.collect()))

  def generateClientsDataFrame: DataFrame = sqlContext.createDataFrame(generateClients)

  def generateOrdersDataFrame(clientsDF: DataFrame): DataFrame = {
    val clientsRDD = clientsDF.rdd.map {
      case Row(id: Int, name: String, forname: String, age: Int) => Client(id, name, forname, age)
    }
    sqlContext.createDataFrame(generateOrdersRDD(clientsRDD))
  }

  def injectNaAgeValues(df: DataFrame): DataFrame = {
    def randomNullify(age: Int) = {
      if(Random.nextDouble() <= 0.1) None else Some(age)
    }
    df.withColumn("ageWithNa", udf(randomNullify _).apply(col("age")))
  }

}
