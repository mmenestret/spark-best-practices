package com.axa.dil.utils

/**
  * Created by martin on 15/11/2016.
  */
object Models {

  case class Client(id: Int, name: String, forname: String, age: Int)
  case class ClientWithAdultTag(id: Int, name: String, forname: String, age: Int, adultTag: Boolean)

  case class Order(id: Int, clientId: Int, numberOfPieces: Int, price: Double)

}
