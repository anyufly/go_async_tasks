package tasks

type Retries struct {
	RetryEscape RetryEscape // 当发生该字段定义的异常时，不重试
	RetryWhen   RetryWhen   //重试条件
}

func (r Retries) apply(t *Task) {
	r.RetryEscape = t.Options.RetryEscape
	r.RetryWhen = t.Options.RetryWhen
}
