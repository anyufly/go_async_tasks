package rabbitmq

import "crypto/tls"

type brokerOptions struct {
	tlsConfig   *tls.Config
	confirm     bool
	maxPriority uint
	usePlugin   bool
}

type IBrokerOption interface {
	apply(b *Broker)
}

type Confirm bool

func (c Confirm) apply(b *Broker) {
	b.options.confirm = bool(c)
}

type MaxPriority uint

func (m MaxPriority) apply(b *Broker) {
	b.options.maxPriority = uint(m)
}

type Tls tls.Config

func (t *Tls) apply(b *Broker) {
	b.options.tlsConfig = (*tls.Config)(t)
}

type UsePlugin bool

func (c UsePlugin) apply(b *Broker) {
	b.options.usePlugin = bool(c)
}
