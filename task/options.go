package tasks

type RetryWhen func(retries int, err error) bool
type RetryEscape []error

type ITaskOption interface {
	apply(t *Task)
}

type taskOption struct {
	RetryEscape RetryEscape // 当发生该字段定义的异常时，不重试
	RetryWhen   RetryWhen   //重试条件
	OnSucess    string
	OnFailure   string
}
