# How to use
- docker pull apache/kafka
- Run the producer
  `cd <dir>/Producer
  go run .`
- Run the consumer
  `cd <dir>/Consumer
  go run .`
- POST http://localhost:3000/order
  Payload: `{
    "customer_name": "Alex",
    "coffee_type": "Iced Americano"   

}`

# Components
- Producer - Hosts the API /orders. The orders will then push a message to kafka
- Consumer - Will listen to kafka messages and prints a message if a message was found
