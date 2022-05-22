package messages

import "time"

type messageOptions struct {
	Delay          time.Duration `json:"delay"`           // 消息延迟时间
	Priority       uint8         `json:"priority"`        //消息优先级 0-255
	Retries        int           `json:"retries"`         //已重试次数
	MaxRetries     int           `json:"max_retries"`     // 最大重试次数
	MaxBackoff     time.Duration `json:"max_backoff"`     // 最大重试间隔 （单位ms）
	MinBackoff     time.Duration `json:"min_backoff"`     // 最小重试间隔 （单位ms）
	PipelineID     string        `json:"pipe_id"`         // pipe_id
	PipeTarget     *Message      `json:"pipe_target"`     //Pipe任务下一条消息
	PipeIgnore     bool          `json:"pipe_ignore"`     //是否忽略管道任务
	PipeLast       bool          `json:"pipe_last"`       // 是否为pipeline的最后一个任务
	GroupID        string        `json:"group_id"`        // group ID
	GroupCallbacks []*Message    `json:"group_callbacks"` //group 回调
}

type IMessageOption interface {
	apply(m *Message)
}
