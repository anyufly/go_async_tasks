package messages

import "time"

type Retries struct {
	MaxRetries int           // 最大重试次数
	MaxBackoff time.Duration // 最大重试间隔 （单位ms）
	MinBackoff time.Duration // 最小重试间隔 （单位ms）
}

func (r Retries) apply(m *Message) {
	m.Options.MaxRetries = r.MaxRetries
	m.Options.MaxBackoff = r.MaxBackoff
	m.Options.MinBackoff = r.MinBackoff
}
