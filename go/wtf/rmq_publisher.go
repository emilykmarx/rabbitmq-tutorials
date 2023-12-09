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
 * Makes a queue, publishes json message to it with fields given by args.
 */
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		// Match logstash plugin
		"logstash", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFromArgs()
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
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
	exch_only_arg := flag.Bool("exch-only", false, "Don't send a message, just create the exchange")
	msg_arg := flag.String("msg", "hello", "Message string (default hello)")
	json_arg := flag.Bool("json", false, `Whether message string is json.
																				Will be unnested, all fields assumed string`)
	slow_arg := flag.Bool("slow", false, "Whether to process this message slowly")
	flag.Parse()
	if *exch_only_arg {
		return nil
	}

	body := map[string]interface{}{
		"slow": *slow_arg,
	}

	if *json_arg {
		var msg_json map[string]string
		err := json.Unmarshal([]byte(*msg_arg), &msg_json)
		if err != nil {
			failOnError(err, "could not unmarshal json msg string")
		}
		for k, v := range msg_json {
			body[k] = v
		}
	} else {
		body["msg"] = *msg_arg
	}

	body_arr, err := json.Marshal(body)
	if err != nil {
		failOnError(err, "could not marshal json")
	}
	return body_arr
}
