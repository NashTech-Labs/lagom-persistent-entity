package com.knoldus.lagompersistententity.impl.service

import akka.util.Timeout
import akka.{Done, NotUsed}
import com.knoldus.lagompersistententity.api.{LagomPersistentEntityService, Product}
import com.knoldus.lagompersistententity.impl.eventSourcing.{AddProductCommand, InventoryCommand, InventoryEntity}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraSession
import com.lightbend.lagom.scaladsl.persistence.{PersistentEntityRef, PersistentEntityRegistry}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Implementation of the LagomPersistentEntityService.
 */
class LagomPersistentEntityServiceImpl(persistentEntityRegistry: PersistentEntityRegistry,session: CassandraSession)
                                      (implicit ec: ExecutionContext) extends LagomPersistentEntityService {

  implicit val timeout: Timeout = Timeout(5.seconds)
  val config: Config = ConfigFactory.load()
  val TABLE_NAME: String = config.getString("cassandra.tableName")
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

  def getProductById(id: String): Future[Option[Product]] =
    session.selectOne(s"SELECT * FROM $TABLE_NAME WHERE id = '$id'").map{rows =>
      rows.map{row =>
        val id = row.getString("id")
        val name = row.getString("name")
        val quantity = row.getLong("quantity")
        Product(id, name, quantity )
      }
    }


  override def getProduct(id: String): ServiceCall[NotUsed, String] = {
    ServiceCall { _ =>
     getProductById(id).map(product =>  s"Product named ${product.get.name} has id: $id and quantity in inventory: ${product.get.quantity}")
    }
  }
}
