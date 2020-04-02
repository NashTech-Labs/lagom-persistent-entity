package com.knoldus.lagompersistententity.impl.service
import com.knoldus.lagompersistententity.api.Product
import com.knoldus.lagompersistententity.impl.eventSourcing.{AddProductCommand, GetProductCommand, InventoryState, ProductAdded}
import com.lightbend.lagom.scaladsl.playjson.{JsonSerializer, JsonSerializerRegistry}

import scala.collection.immutable

object UserSerializerRegistry extends JsonSerializerRegistry {
  override def serializers: immutable.Seq[JsonSerializer[_]] = immutable.Seq(
    JsonSerializer[Product],
    JsonSerializer[AddProductCommand],
    JsonSerializer[GetProductCommand],
    JsonSerializer[ProductAdded],
    JsonSerializer[InventoryState]
  )
}
