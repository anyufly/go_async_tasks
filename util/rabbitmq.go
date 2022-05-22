package util

import (
	"crypto/tls"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConnector struct {
	url     string
	options rabbitMQConnectorOption
}

type rabbitMQConnectorOption struct {
	tlsConfig *tls.Config
}

type TlsOption tls.Config

func (t *TlsOption) apply(c *RabbitMQConnector) {
	c.options.tlsConfig = (*tls.Config)(t)
}

type IRabbitMQConnectorOptions interface {
	apply(c *RabbitMQConnector)
}

func NewRabbitMQConnector(url string) *RabbitMQConnector {
	return &RabbitMQConnector{
		url: url,
	}
}

func (c *RabbitMQConnector) IsTlsConnector() bool {
	return c.options.tlsConfig == nil
}

func (c *RabbitMQConnector) WithOptions(options ...IRabbitMQConnectorOptions) *RabbitMQConnector {
	for _, opt := range options {
		opt.apply(c)
	}
	return c
}

func (c *RabbitMQConnector) Open() (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	if c.IsTlsConnector() {
		conn, err = amqp.DialTLS(c.url, c.options.tlsConfig)
	} else {
		conn, err = amqp.Dial(c.url)
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *RabbitMQConnector) Close(conn *amqp.Connection) error {
	if !conn.IsClosed() {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
