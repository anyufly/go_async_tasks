package backend

import (
	"context"

	"github.com/fakerjeff/go_async_tasks/iface/task"
)

type IBackend interface {
	Save(ctx context.Context, messageID string, result []*task.Result) error
	Get(ctx context.Context, messageID string) ([]*task.Result, error)
	Completed(ctx context.Context, messageID string) (bool, error)
	SaveGroup(ctx context.Context, groupID string, groupCount int, childs []string) error
	DeleteGroup(ctx context.Context, groupID string) error
	GroupResult(ctx context.Context, groupID string) (map[string][]*task.Result, error)
	GroupCompleteOne(ctx context.Context, groupID string, messageID string, result []*task.Result) error
	GroupCompleteCount(ctx context.Context, groupID string) (int64, error)
	GroupCompleted(ctx context.Context, groupID string) (bool, error)
	SavePipeline(ctx context.Context, pipelineID, lastMessageID string) error
	PipelineResult(ctx context.Context, pipelineID string) ([]*task.Result, error)
	PipelineCompleted(ctx context.Context, pipelineID string) (bool, error)
	DeletePipeline(ctx context.Context, pipelineID string) error
}
