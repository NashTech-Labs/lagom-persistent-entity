package com.knoldus.lagompersistententity.impl.eventSourcing

import play.api.libs.json.{Json, Format}
import com.knoldus.lagompersistententity.api.Product

case class InventoryState(product: Option[Product], time: String)

object InventoryState {
  implicit val format: Format[InventoryState] = Json.format[InventoryState]
}
