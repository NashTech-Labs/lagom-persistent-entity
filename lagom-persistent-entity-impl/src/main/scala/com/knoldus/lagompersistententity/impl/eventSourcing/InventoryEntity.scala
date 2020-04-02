package com.knoldus.lagompersistententity.impl.eventSourcing

import java.time.LocalDateTime

import akka.Done
import com.knoldus.lagompersistententity.api.Product
import com.lightbend.lagom.scaladsl.persistence.PersistentEntity

class InventoryEntity extends PersistentEntity {
  override type Command = InventoryCommand[_]
  override type Event = InventoryEvent
  override type State = InventoryState

  override def initialState: InventoryState = InventoryState(None, LocalDateTime.now().toString)

  override def behavior: InventoryState => Actions = {
    case InventoryState(_, _) => Actions()
      .onEvent {
        case (ProductAdded(product), _) =>
          InventoryState(Some(product), LocalDateTime.now().toString)
      }
      .onReadOnlyCommand[GetProductCommand, Product] {
        case (GetProductCommand(id), ctx, state) =>
          ctx.reply(state.product.getOrElse(Product(id, "not found", -1)))
      }
      .onCommand[AddProductCommand, Done] {
        case (AddProductCommand(product), ctx, _) =>
          ctx.thenPersist(ProductAdded(product))(_ ⇒ ctx.reply(Done))
      }

  }
}