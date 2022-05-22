package messages

import "time"

type MessageDelay time.Duration

func (d MessageDelay) apply(m *Message) {
	m.Options.Delay = time.Duration(d)
	etaTime := time.Now().Local().Add(m.Options.Delay)
	m.ETA = etaTime.UnixNano()
}
