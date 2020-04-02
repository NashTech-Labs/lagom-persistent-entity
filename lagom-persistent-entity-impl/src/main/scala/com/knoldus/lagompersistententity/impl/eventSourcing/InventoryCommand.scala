package com.knoldus.lagompersistententity.impl.eventSourcing

import akka.Done
import com.lightbend.lagom.scaladsl.persistence.PersistentEntity.ReplyType
import play.api.libs.json.{Format, Json}

import com.knoldus.lagompersistententity.api.Product

trait InventoryCommand[R] extends ReplyType[R]

case class AddProductCommand(product: Product) extends InventoryCommand[Done]

object AddProductCommand {
  implicit val format: Format[AddProductCommand] = Json.format[AddProductCommand]
}

case class GetProductCommand(id: String) extends InventoryCommand[Product]

object GetProductCommand {
  implicit val format: Format[GetProductCommand] = Json.format[GetProductCommand]
}
