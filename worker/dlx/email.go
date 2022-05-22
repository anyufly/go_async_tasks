package dlx

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type EmialTask struct {
}

func (e EmialTask) Name() string {
	return "go_async_tasks.worker.dlx.EmailTask"
}

func (e EmialTask) Call(args []task.Arg) (taskResults []*task.Result, err error) {
	panic("implement me")
}

func (e EmialTask) WithCallContext(ctx context.Context) task.ITask {
	panic("implement me")
}
