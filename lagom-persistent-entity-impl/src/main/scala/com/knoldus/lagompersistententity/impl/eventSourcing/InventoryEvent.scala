package com.knoldus.lagompersistententity.impl.eventSourcing

import com.knoldus.lagompersistententity.api.Product
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventShards, AggregateEventTag, AggregateEventTagger}
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.json.{Format, Json}

sealed trait InventoryEvent extends AggregateEvent[InventoryEvent] {
  override def aggregateTag: AggregateEventTagger[InventoryEvent] = InventoryEvent.Tag
}

object InventoryEvent {
  val config: Config = ConfigFactory.load()
  val numberOfShards: Int = config.getInt("cassandra.shards")
  val Tag: AggregateEventShards[InventoryEvent] = AggregateEventTag.sharded[InventoryEvent](numberOfShards)
}

case class ProductAdded(product: Product) extends InventoryEvent

object ProductAdded {
  implicit val format: Format[ProductAdded] = Json.format[ProductAdded]
}
