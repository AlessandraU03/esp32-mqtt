package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Cargar las variables de entorno desde .env
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error cargando el archivo .env: %v", err)
	}

	mqttBroker := os.Getenv("MQTT_BROKER")
	mqttUsername := os.Getenv("MQTT_USERNAME")
	mqttPassword := os.Getenv("MQTT_PASSWORD")
	amqpServer := os.Getenv("AMQP_SERVER")
	amqpQueue := os.Getenv("AMQP_QUEUE")

	// Conectar a RabbitMQ
	connRabbit, err := amqp.Dial(amqpServer)
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	defer connRabbit.Close()

	chRabbit, err := connRabbit.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal en RabbitMQ: %v", err)
	}
	defer chRabbit.Close()

	_, err = chRabbit.QueueDeclare(amqpQueue, false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declarando cola en RabbitMQ: %v", err)
	}

	// Conectar a MQTT
	opts := mqtt.NewClientOptions().AddBroker(mqttBroker)
	opts.SetUsername(mqttUsername).SetPassword(mqttPassword)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error conectando a MQTT: %v", token.Error())
	}

	fmt.Println("Conectado a MQTT")

	// Suscribirse al topic MQTT
	client.Subscribe("esp32/sensores", 0, func(client mqtt.Client, msg mqtt.Message) {
		message := string(msg.Payload())

		// Publicar en RabbitMQ
		err = chRabbit.Publish("", amqpQueue, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(message),
		})
		if err != nil {
			log.Printf("Error enviando mensaje a RabbitMQ: %v", err)
		} else {
			fmt.Println("Mensaje reenviado a RabbitMQ:", message)
		}
	})

	select {} // Mantener el proceso en ejecución
}
