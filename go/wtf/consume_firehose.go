package main

import (
	"log"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

/*
 * Log all messages published/delivered on the server
 */
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"consume_firehose", // name
		false,              // durable
		false,              // delete when unused
		true,               // exclusive
		false,              // no-wait
		nil,                // arguments
	)

	failOnError(err, "Failed to declare a queue")

	exch := "amq.rabbitmq.trace"
	err = ch.QueueBind(
		q.Name, // queue name
		"#",    // routing key: trace all
		exch,   // exchange
		false,
		nil)

	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			msg_info := ""
			key_tokens := strings.Split(d.RoutingKey, ".")
			if key_tokens[0] == "deliver" {
				msg_info = "Delivered to queue " + key_tokens[1]
			} else {
				msg_info = "Published to exchange " + key_tokens[1]
			}
			log.Printf(" [x] %v\n\nHeaders: %v\n\nBody: %s", msg_info, d.Headers, d.Body)
		}
	}()

	log.Printf(" [*] Waiting for msgs. To exit press CTRL+C")
	<-forever
}
