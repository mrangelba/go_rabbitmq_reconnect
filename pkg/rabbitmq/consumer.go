package rabbitmq

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	uri           string
	conn          *amqp.Connection
	channel       *amqp.Channel
	queue         *amqp.Queue
	exchange      string
	exchangeType  string
	queueName     string
	tag           string
	handle        handle
	reconnect     bool
	retryAwait    time.Duration
	retryAttempts int
	notifyClosed  chan error
	Done          chan error
}

type handle func(deliveries <-chan amqp.Delivery, done chan error)

func (c *Consumer) dial() error {
	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("sample-consumer")

	c.conn, err = amqp.DialConfig(c.uri, config)
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	log.Printf("Connected (%s)\n", c.uri)

	go func() {
		notify := <-c.notifyClosed

		if c.reconnect {
			var i = 0
			for {
				i++
				log.Printf("Reconnecting %v (%s)\n", i, c.uri)

				err := c.connect()

				if err == nil {
					break
				} else {
					time.Sleep(c.retryAwait)
				}

				if c.retryAttempts > 0 && i == c.retryAttempts {
					c.Done <- nil

					break
				}
			}
		} else {
			c.Done <- notify
		}
	}()

	return nil
}

func (c *Consumer) openChannel() error {
	var err error

	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	return nil
}

func (c *Consumer) bindQueue() error {
	var err error

	if err = c.channel.ExchangeDeclare(
		c.exchange,     // name of the exchange
		c.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}

	queue, err := c.channel.QueueDeclare(
		c.queueName, // name of the queue
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)

	c.queue = &queue

	if err != nil {
		return fmt.Errorf("queue declare: %s", err)
	}

	if err = c.channel.QueueBind(
		c.queue.Name, // name of the queue
		"",           // bindingKey
		c.exchange,   // sourceExchange
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("queue bind: %s", err)
	}

	return nil
}

func (c *Consumer) consume() error {
	deliveries, err := c.channel.Consume(
		c.queue.Name, // name
		c.tag,        // consumerTag,
		true,         // autoAck
		false,        // exclusive
		false,        // noLocal
		false,        // noWait
		nil,          // arguments
	)

	if err != nil {
		return fmt.Errorf("queue consume: %s", err)
	}

	go c.handle(deliveries, c.notifyClosed)

	return nil
}

func (c *Consumer) connect() error {
	err := c.dial()

	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	err = c.openChannel()

	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	err = c.bindQueue()

	if err != nil {
		return fmt.Errorf("bind-queue: %s", err)
	}

	err = c.consume()

	if err != nil {
		return fmt.Errorf("consume: %s", err)
	}

	return nil
}

func NewConsumer(uri, exchange, exchangeType, queueName, ctag string, attempts int, handle handle) (*Consumer, error) {
	c := &Consumer{
		uri:           uri,
		conn:          nil,
		channel:       nil,
		queue:         nil,
		exchange:      exchange,
		exchangeType:  exchangeType,
		queueName:     queueName,
		tag:           ctag,
		handle:        handle,
		reconnect:     true,
		retryAwait:    10 * time.Second,
		retryAttempts: attempts,
		Done:          make(chan error),
		notifyClosed:  make(chan error),
	}

	err := c.connect()

	if err != nil {
		return nil, err
	}

	return c, nil
}
