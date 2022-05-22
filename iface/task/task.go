package task

type Arg struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type Result struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type ITask interface {
	Name() string
	Call(args []Arg) (taskResults []*Result, err error)
}
