// mqtt_to_rabbitmq.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/joho/godotenv"
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
	exchangeName := "sensor_data"
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

	// Declarar Exchange de tipo "direct"
	err = chRabbit.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declarando exchange: %v", err)
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
	client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		var sensorData map[string]interface{}
		if err := json.Unmarshal(msg.Payload(), &sensorData); err != nil {
			log.Printf("Error al deserializar el mensaje: %v", err)
			return
		}

		sensorType, ok := sensorData["sensor"].(string)
		if !ok {
			log.Println("Mensaje inválido, no contiene tipo de sensor")
			return
		}

		message, _ := json.Marshal(sensorData)

		// Publicar mensaje en el Exchange con la Routing Key correspondiente
		err := chRabbit.Publish(exchangeName, sensorType, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})
		if err != nil {
			log.Printf("Error enviando mensaje a RabbitMQ: %v", err)
		} else {
			fmt.Printf("Mensaje enviado a RabbitMQ [%s]: %s\n", sensorType, message)
		}
	})

	select {} // Mantener el proceso en ejecución
}
