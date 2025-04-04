package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Cargar variables de entorno
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error cargando .env: %v", err)
	}

	mqttBroker := os.Getenv("MQTT_BROKER")
	mqttUsername := os.Getenv("MQTT_USERNAME")
	mqttPassword := os.Getenv("MQTT_PASSWORD")
	amqpServer := os.Getenv("AMQP_SERVER")
	topic := "esp32/sensores"

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

	// Declarar una cola en RabbitMQ (sin importar el tipo de sensor)
	queueName := "sensores"
	_, err = chRabbit.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declarando la cola: %v", err)
	}

	// Conectar a MQTT
	opts := mqtt.NewClientOptions().AddBroker(mqttBroker)
	opts.SetUsername(mqttUsername).SetPassword(mqttPassword)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error conectando a MQTT: %v", token.Error())
	}

	fmt.Println("Conectado a MQTT")

	// Subscribirse al tema MQTT y procesar mensajes
	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		var sensorData map[string]interface{}
		if err := json.Unmarshal(msg.Payload(), &sensorData); err != nil {
			log.Printf("Error al deserializar el mensaje: %v", err)
			return
		}

		// Convertir el mensaje a JSON y enviarlo a RabbitMQ (a la misma cola)
		message, err := json.Marshal(sensorData)
		if err != nil {
			log.Printf("Error serializando el mensaje: %v", err)
			return
		}

		// Publicar el mensaje en RabbitMQ
		err = chRabbit.Publish(
			"",            // Exchange
			queueName,     // Queue name
			false,         // Mandatory
			false,         // Immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        message,
			},
		)
		if err != nil {
			log.Printf("Error enviando mensaje a RabbitMQ: %v", err)
		} else {
			fmt.Printf("Mensaje enviado a RabbitMQ: %s\n", message)
		}
	})

	// Mantener el programa en ejecución
	select {}
}
