package messages

type MessagePriority uint8

func (p MessagePriority) apply(m *Message) {
	m.Options.Priority = uint8(p)
}
