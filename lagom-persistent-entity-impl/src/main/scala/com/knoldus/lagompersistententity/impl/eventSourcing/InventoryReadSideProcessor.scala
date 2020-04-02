package com.knoldus.lagompersistententity.impl.eventSourcing

import akka.Done
import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import com.lightbend.lagom.scaladsl.persistence.{AggregateEventTag, ReadSideProcessor}
import com.lightbend.lagom.scaladsl.persistence.cassandra.{CassandraReadSide, CassandraSession}
import com.knoldus.lagompersistententity.api.Product
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}

class InventoryReadSideProcessor(cassandraSession: CassandraSession, readSide: CassandraReadSide)(implicit ec: ExecutionContext) extends ReadSideProcessor[InventoryEvent] {
  override def aggregateTags: Set[AggregateEventTag[InventoryEvent]] =
    InventoryEvent.Tag.allTags

  val config: Config = ConfigFactory.load()
  val TABLE_NAME: String = config.getString("cassandra.tableName")
  var addEntity: PreparedStatement = _

  /**
   * This method on the ReadSideProcessor is buildHandler.
   * This is responsible for creating the ReadSideHandler that will handle events
   *
   * @return
   */
  override def buildHandler(): ReadSideProcessor.ReadSideHandler[InventoryEvent] =
    readSide.builder[InventoryEvent]("inventoryOffset")
      .setGlobalPrepare(createTable)
      .setPrepare(_ => prepareStatements())
      .setEventHandler[ProductAdded](ese => addEntity(ese.event.product))
      .build()

  /**
   * Creates a table at the start of the application.
   *
   * @return Future[Done]
   */
  def createTable(): Future[Done] = {
    cassandraSession.executeCreateTable(
      s"""
         |CREATE TABLE IF NOT EXISTS $TABLE_NAME(
         |id text PRIMARY KEY,
         |name text,
         |age int
         |);
      """.stripMargin)
  }

  /**
   * This will be executed once per shard, when the read side processor starts up.
   * It can be used for preparing statements in order to optimize Cassandra’s handling of them.
   *
   * @return Future[Done]
   */
  def prepareStatements(): Future[Done] =
    cassandraSession.prepare(s"INSERT INTO $TABLE_NAME(id, name, age) VALUES (?, ?, ?)")
      .map { ps =>
        addEntity = ps
        Done
      }

  def addEntity(product: Product): Future[List[BoundStatement]] = {
    val bindInsertProduct: BoundStatement = addEntity.bind()
    bindInsertProduct.setString("id", product.id)
    bindInsertProduct.setString("name", product.name)
    bindInsertProduct.setLong("quantity", product.quantity)
    Future.successful(List(bindInsertProduct))
  }
}

