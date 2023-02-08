package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	uri           string
	conn          *amqp.Connection
	channel       *amqp.Channel
	queue         *amqp.Queue
	exchange      string
	exchangeType  string
	reconnect     bool
	retryAwait    time.Duration
	retryAttempts int
	notifyClosed  chan error
	Done          chan error
}

func (p *Producer) dial() error {
	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("sample-producer")

	p.conn, err = amqp.DialConfig(p.uri, config)
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	log.Printf("Connected (%s)\n", p.uri)

	go func() {
		p.notifyClosed <- <-p.conn.NotifyClose(make(chan *amqp.Error))
	}()

	go func() {
		notify := <-p.notifyClosed

		if p.reconnect {
			var i = 0
			for {
				i++
				log.Printf("Reconnecting %v (%s)\n", i, p.uri)

				err := p.connect()

				if err == nil {
					break
				} else {
					time.Sleep(p.retryAwait)
				}

				if p.retryAttempts > 0 && i == p.retryAttempts {
					p.Done <- nil

					break
				}
			}
		} else {
			p.Done <- notify
		}
	}()

	return nil
}

func (p *Producer) openChannel() error {
	var err error

	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	return nil
}

func (p *Producer) declareExchange() error {
	var err error

	if err = p.channel.ExchangeDeclare(
		p.exchange,     // name of the exchange
		p.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}

	return nil
}

func (p *Producer) Publish(message string) error {
	log.Printf("Publishing message: %s\n", message)

	return p.channel.PublishWithContext(
		context.Background(),
		p.exchange, "",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
}

func (p *Producer) connect() error {
	err := p.dial()

	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	err = p.openChannel()

	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	err = p.declareExchange()

	if err != nil {
		return fmt.Errorf("exchange: %s", err)
	}

	return nil
}

func NewProducer(uri, exchange, exchangeType string, attempts int) (*Producer, error) {
	p := &Producer{
		uri:           uri,
		conn:          nil,
		channel:       nil,
		queue:         nil,
		exchange:      exchange,
		exchangeType:  exchangeType,
		reconnect:     true,
		retryAwait:    10 * time.Second,
		retryAttempts: attempts,
		Done:          make(chan error),
		notifyClosed:  make(chan error),
	}

	err := p.connect()

	if err != nil {
		return nil, err
	}

	return p, nil
}
