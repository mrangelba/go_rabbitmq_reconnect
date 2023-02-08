// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
package main

import (
	"log"
	"sync"

	"github.com/devfullcycle/fcutils/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	amqpURI = "amqp://guest:guest@localhost:5672/"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		c1, err := rabbitmq.NewConsumer(amqpURI, "test-exchange", "fanout", "test-queue", "simple-consumer", 3, handle)
		if err != nil {
			log.Fatalf("%s", err)
		}

		<-c1.Done
		wg.Done()
	}()

	go func() {
		c2, err := rabbitmq.NewConsumer(amqpURI, "test-exchange-2", "fanout", "test-queue-2", "simple-consumer-2", 2, handle)
		if err != nil {
			log.Fatalf("%s", err)
		}

		<-c2.Done
		wg.Done()
	}()

	wg.Wait()
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	cleanup := func() {
		done <- nil
	}

	defer cleanup()

	for d := range deliveries {
		log.Printf(
			"delivery: %q",
			d.Body,
		)
	}
}
