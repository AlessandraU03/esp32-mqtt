package main

import (
	"fmt"
	"log"

       amqp "github.com/rabbitmq/amqp091-go"
)

const amqpQueue = "sensor_data"

func main() {
	// Conectar a RabbitMQ
	connRabbit, err := amqp.Dial("amqp://ale:ale05@54.156.170.232:5672/")
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	defer connRabbit.Close()

	ch, err := connRabbit.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal en RabbitMQ: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(amqpQueue, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error consumiendo cola: %v", err)
	}

	fmt.Println("Esperando mensajes...")

	for msg := range msgs {
		fmt.Println("Recibido desde RabbitMQ:", string(msg.Body))
	}
}
