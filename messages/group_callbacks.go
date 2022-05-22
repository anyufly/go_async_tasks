package messages

type GroupCallbacks struct {
	GroupID        string
	GroupCallbacks []*Message
}

func (g GroupCallbacks) apply(m *Message) {
	m.Options.GroupID = g.GroupID
	m.Options.GroupCallbacks = g.GroupCallbacks
}
