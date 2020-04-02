package com.knoldus.lagompersistententity.api

import play.api.libs.json.{Format, Json}

case class Product(id:String,name:String,quantity:Long)

object Product{
  implicit val format: Format[Product] = Json.format[Product]
}
