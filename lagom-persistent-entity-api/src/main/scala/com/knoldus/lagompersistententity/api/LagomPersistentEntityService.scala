package com.knoldus.lagompersistententity.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.transport.Method
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}


/**
 * The lagom-persistent-entity service interface.
 * <p>
 * This describes everything that Lagom needs to know about how to serve and
 * consume the LagomPersistentEntityService.
 */
trait LagomPersistentEntityService extends Service {

  def addProduct(id: String, name: String, quantity: Long): ServiceCall[Product,String]

  def getProduct(id: String): ServiceCall[NotUsed, String]

  override def descriptor: Descriptor = {
    import Service._

    named("inventory_service")
      .withCalls(
        restCall(Method.POST, "/inventory/add/:id/:name/:quantity", addProduct _),
        restCall(Method.GET, "/inventory/details/:id", getProduct _)
      ).withAutoAcl(true)
  }
}
