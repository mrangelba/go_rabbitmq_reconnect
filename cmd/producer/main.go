package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/devfullcycle/fcutils/pkg/rabbitmq"
)

var (
	amqpURI = "amqp://guest:guest@localhost:5672/"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		p, err := rabbitmq.NewProducer(amqpURI, "test-exchange", "fanout", 2)

		if err != nil {
			log.Fatalf("%s", err)
		}

		for i := 0; i < 100; i++ {
			p.Publish(fmt.Sprintf("Message exchange %v", i))
			time.Sleep(5 * time.Second)
		}

		<-p.Done
		wg.Done()
	}()

	go func() {
		p, err := rabbitmq.NewProducer(amqpURI, "test-exchange-2", "fanout", 2)

		if err != nil {
			log.Fatalf("%s", err)
		}

		for i := 0; i < 100; i++ {
			p.Publish(fmt.Sprintf("Message exchange-2 %v", i))
			time.Sleep(10 * time.Second)
		}

		<-p.Done
		wg.Done()
	}()

	wg.Wait()
}
