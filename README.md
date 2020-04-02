# lagom-persistent-entity

This is a simple project in which there is an inventory in which product are added and you can fetch the details of the product.

Steps to run this project
- open your terminal
- clone this project
- go to directory lagom-persistent-entity
- sbt clean compile
- sbt runAll

You can check which services are up by hitting http://localhost:9008/services

You can now hit the service using insomnia-

To add a product in the inventory, hit the service with a POST call using URL - http://localhost:9000/inventory/add/:id/:name/:quantity.In body you will have to mention a JSON. 
For example -
JSON will be:
{
	"id": "23",
	"name": "Pen",
	"quantity": 10
}

and call will be:
http://localhost:9000/inventory/add/23/Pen/10

To fetch product details from the inventory, hit the service with a GET call using URL - http://localhost:9000/inventory/details/:id.
For example-
call will be:
http://localhost:9000/inventory/details/23
