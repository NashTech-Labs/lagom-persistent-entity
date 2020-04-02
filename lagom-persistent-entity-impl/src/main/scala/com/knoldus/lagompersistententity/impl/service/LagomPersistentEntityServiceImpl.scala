package com.knoldus.lagompersistententity.impl.service

import akka.util.Timeout
import akka.{Done, NotUsed}
import com.knoldus.lagompersistententity.api.{LagomPersistentEntityService, Product}
import com.knoldus.lagompersistententity.impl.eventSourcing.{
  AddProductCommand, GetProductCommand
  , InventoryCommand, InventoryEntity
}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.persistence.{PersistentEntityRef, PersistentEntityRegistry}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Implementation of the LagomPersistentEntityService.
 */
class LagomPersistentEntityServiceImpl(persistentEntityRegistry: PersistentEntityRegistry)
                                      (implicit ec: ExecutionContext) extends LagomPersistentEntityService {

  implicit val timeout: Timeout = Timeout(5.seconds)

  /**
   * Looks up the entity for the given ID.
   */
  def ref(id: String): PersistentEntityRef[InventoryCommand[_]] = {
    persistentEntityRegistry
      .refFor[InventoryEntity](id)
  }

  override def addProduct(id: String, name: String, quantity: Long): ServiceCall[Product, String] = {
    ServiceCall { _ =>
      val product = Product(id, name, quantity)
      ref(product.id).ask(AddProductCommand(product)).map {
        case Done => s"$quantity $name have been added to the inventory"
      }
    }
  }

  override def getProduct(id: String): ServiceCall[NotUsed, String] = {
    ServiceCall { _ =>
      ref(id).ask(GetProductCommand(id)).map(product =>
        s"Product corresponding to id:$id is ${product.name}")
    }
  }

}
