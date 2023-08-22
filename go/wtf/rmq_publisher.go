package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

/*
 * Makes a fanout exchange called "logs", publishes json message to it with fields given by args.
 */
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFromArgs()
	err = ch.PublishWithContext(ctx,
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func bodyFromArgs() []byte {
	msg_arg := flag.String("msg", "hello", "Message string (default hello)")
	slow_arg := flag.Bool("slow", false, "Whether to process this message slowly")
	flag.Parse()

	body := map[string]interface{}{
		"msg":  *msg_arg,
		"slow": *slow_arg,
	}
	body_arr, err := json.Marshal(body)
	if err != nil {
		failOnError(err, "could not marshal json")
	}
	return body_arr
}
