package messages

type Pipeline struct {
	PipelineID string
	PipeTarget *Message
	PipeIgnore bool
	PipeLast   bool
}

func (p Pipeline) apply(m *Message) {
	m.Options.PipelineID = p.PipelineID
	m.Options.PipeTarget = p.PipeTarget
	m.Options.PipeIgnore = p.PipeIgnore
	m.Options.PipeLast = p.PipeLast
}
