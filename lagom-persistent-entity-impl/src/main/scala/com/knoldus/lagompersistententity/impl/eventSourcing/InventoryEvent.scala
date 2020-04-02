package com.knoldus.lagompersistententity.impl.eventSourcing

import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, AggregateEventTagger}
import play.api.libs.json.{Format, Json}
import com.knoldus.lagompersistententity.api.Product

sealed trait InventoryEvent extends AggregateEvent[InventoryEvent] {
  override def aggregateTag: AggregateEventTagger[InventoryEvent] = InventoryEvent.Tag
}

object InventoryEvent {
  val Tag: AggregateEventTag[InventoryEvent] = AggregateEventTag[InventoryEvent]
}

case class ProductAdded(product: Product) extends InventoryEvent

object ProductAdded {
  implicit val format: Format[ProductAdded] = Json.format[ProductAdded]
}
