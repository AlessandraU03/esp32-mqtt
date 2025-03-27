// rabbitmq_consumer.go
package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/joho/godotenv"
)

func main() {
	// Cargar variables de entorno
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error cargando .env: %v", err)
	}

	amqpServer := os.Getenv("AMQP_SERVER")
	exchangeName := "sensor_data"
	queueName := "sensor_queue"
	bindingKey := "temperatura" // Cambia según el tipo de sensor que quieras consumir

	// Conectar a RabbitMQ
	conn, err := amqp.Dial(amqpServer)
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal en RabbitMQ: %v", err)
	}
	defer ch.Close()

	// Declarar la cola y enlazarla con la routing key correspondiente
	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declarando cola: %v", err)
	}

	err = ch.QueueBind(q.Name, bindingKey, exchangeName, false, nil)
	if err != nil {
		log.Fatalf("Error vinculando la cola: %v", err)
	}

	// Consumir mensajes
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error iniciando consumo: %v", err)
	}

	fmt.Printf("Esperando mensajes de tipo [%s]...\n", bindingKey)

	for msg := range msgs {
		fmt.Printf("Mensaje recibido: %s\n", msg.Body)
	}
}
