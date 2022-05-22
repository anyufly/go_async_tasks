package consts

const DefaultTimeFmt = "2006-01-02 15:04:05"

const (
	RetriesMiddleware        = "go_async_task.middleware.Retries"
	CallbacksMiddleware      = "go_async_task.middleware.Callbacks"
	CurrentMessageMiddleware = "go_async_task.middleware.CurrentMessage"
	GroupMiddleware          = "go_async_task.middleware.Group"
	PipelineMiddleware       = "go_async_task.middleware.Pipeline"
	PeriodMiddleware         = "go_async_task.middleware.Period"
	ResultsMiddleware        = "go_async_task.middleware.Results"
)
