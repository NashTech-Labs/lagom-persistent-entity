package com.knoldus.lagompersistententity.impl.service

import com.knoldus.lagompersistententity.api.LagomPersistentEntityService
import com.knoldus.lagompersistententity.impl.eventSourcing.{InventoryEntity, InventoryReadSideProcessor}
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.api.{Descriptor, ServiceLocator}
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraPersistenceComponents
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.lightbend.lagom.scaladsl.server._
import com.softwaremill.macwire._
import play.api.libs.ws.ahc.AhcWSComponents

class LagomPersistentEntityLoader extends LagomApplicationLoader {

  override def load(context: LagomApplicationContext): LagomApplication =
    new LagomPersistentEntityApplication(context) {
      override def serviceLocator: ServiceLocator = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    new LagomPersistentEntityApplication(context) with LagomDevModeComponents

  override def describeService: Option[Descriptor] = Some(readDescriptor[LagomPersistentEntityService])
}

abstract class LagomPersistentEntityApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
    with CassandraPersistenceComponents
    with AhcWSComponents {

  // Bind the service this server provides
  override lazy val lagomServer: LagomServer = serverFor[LagomPersistentEntityService](wire[LagomPersistentEntityServiceImpl])

  // Register the JSON serializer registry
  override lazy val jsonSerializerRegistry: JsonSerializerRegistry = UserSerializerRegistry
  //Register the persistent entity
  persistentEntityRegistry.register(wire[InventoryEntity])
//Register read side processor
  readSide.register(wire[InventoryReadSideProcessor])

}