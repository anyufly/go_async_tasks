package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/fakerjeff/go_async_tasks/util/json"

	"github.com/fakerjeff/go_async_tasks/util"

	"github.com/fakerjeff/go_async_tasks/errr"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/go-redis/redis/v8"
)

type Backend struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewBackend(rdb *redis.Client, ttl time.Duration) *Backend {
	return &Backend{rdb: rdb, ttl: ttl}
}

func (b *Backend) Save(ctx context.Context, messageID string, result []*task.Result) error {
	r, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err = b.rdb.Set(ctx, b.messageKey(messageID), r, b.ttl).Err(); err != nil {
		return err
	}
	return nil
}

func (b *Backend) Completed(ctx context.Context, messageID string) (bool, error) {
	exists, err := b.rdb.Exists(ctx, b.messageKey(messageID)).Result()
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (b *Backend) Get(ctx context.Context, messageID string) ([]*task.Result, error) {
	result, err := b.rdb.Get(ctx, b.messageKey(messageID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, errr.TaskNotComplete
		}
		return nil, err
	}
	res, err := util.UnMarshalTaskResult([]byte(result), &[]*task.Result{})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *Backend) messageKey(messageID string) string {
	return fmt.Sprintf("message_result:%s", messageID)
}

func (b *Backend) groupChildKey(groupID string) string {
	return fmt.Sprintf("group_child:%s", groupID)
}

func (b *Backend) groupResultKey(groupID string) string {
	return fmt.Sprintf("group_result:%s", groupID)
}

func (b *Backend) groupCompleteKey(groupID string) string {
	return fmt.Sprintf("group_complete:%s", groupID)
}

func (b *Backend) SaveGroup(ctx context.Context, groupID string, groupCount int, childs []string) error {
	c, err := json.Marshal(childs)
	if err != nil {
		return err
	}
	pipe := b.rdb.Pipeline()
	pipe.HSet(ctx, b.groupChildKey(groupID), "child_count", groupCount)
	pipe.HSet(ctx, b.groupChildKey(groupID), "childs", c)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) DeleteGroup(ctx context.Context, groupID string) error {
	if err := b.rdb.HDel(ctx, b.groupChildKey(groupID)).Err(); err != nil {
		return err
	}
	return nil
}

func (b *Backend) GroupResult(ctx context.Context, groupID string) (map[string][]*task.Result, error) {
	completed, err := b.GroupCompleted(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if !completed {
		return nil, errr.GroupNotCompleted
	}

	var vals []string
	iter := b.rdb.HScan(ctx, b.groupResultKey(groupID), 0, "", 100).Iterator()
	for iter.Next(ctx) {
		vals = append(vals, iter.Val())
	}
	var res = make(map[string][]*task.Result)
	if len(vals) > 0 {
		for i, val := range vals {
			if i%2 == 1 {
				continue
			}
			key := val
			v := vals[i+1]
			var value []*task.Result
			value, err = util.UnMarshalTaskResult([]byte(v), &[]*task.Result{})
			if err != nil {
				return nil, err
			}
			res[key] = value
		}
	}
	return res, nil
}

func (b *Backend) GroupCompleteOne(ctx context.Context, groupID string, messageID string, result []*task.Result) error {
	r, err := json.Marshal(result)
	if err != nil {
		return err
	}
	pipe := b.rdb.Pipeline()
	pipe.ZAdd(ctx, b.groupCompleteKey(groupID), &redis.Z{
		Member: messageID,
		Score:  float64(time.Now().UnixNano()),
	})
	pipe.Expire(ctx, b.groupCompleteKey(groupID), b.ttl)
	pipe.Set(ctx, b.messageKey(messageID), r, b.ttl)
	pipe.HSet(ctx, b.groupResultKey(groupID), messageID, r)
	pipe.Expire(ctx, b.groupResultKey(groupID), b.ttl)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) GroupCompleted(ctx context.Context, groupID string) (bool, error) {
	pipe := b.rdb.Pipeline()
	childCount := pipe.HGet(ctx, b.groupChildKey(groupID), "child_count")
	completeCount := pipe.ZCard(ctx, b.groupCompleteKey(groupID))
	_, err := pipe.Exec(ctx)
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	child, e := childCount.Int64()
	if e != nil {
		return false, e
	}
	complete := completeCount.Val()
	return child == complete, nil
}

func (b *Backend) GroupCompleteCount(ctx context.Context, groupID string) (int64, error) {
	completeCount, err := b.rdb.ZCard(ctx, b.groupCompleteKey(groupID)).Result()
	if err != nil {
		return 0, err
	}
	return completeCount, nil
}

func (b *Backend) lastMessageIDKey(piplineID string) string {
	return fmt.Sprintf("pipe_last:%s", piplineID)
}

func (b *Backend) SavePipeline(ctx context.Context, pipelineID, lastMessageID string) error {
	if err := b.rdb.Set(ctx, b.lastMessageIDKey(pipelineID), lastMessageID, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (b *Backend) PipelineResult(ctx context.Context, pipelineID string) ([]*task.Result, error) {
	lastMessageID, err := b.rdb.Get(ctx, b.lastMessageIDKey(pipelineID)).Result()
	if err != nil {
		return nil, err
	}
	return b.Get(ctx, lastMessageID)
}

func (b *Backend) PipelineCompleted(ctx context.Context, pipelineID string) (bool, error) {
	lastMessageID, err := b.rdb.Get(ctx, b.lastMessageIDKey(pipelineID)).Result()
	if err != nil {
		return false, err
	}

	exists, err := b.rdb.Exists(ctx, b.messageKey(lastMessageID)).Result()
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (b *Backend) DeletePipeline(ctx context.Context, pipelineID string) error {
	if err := b.rdb.Del(ctx, b.lastMessageIDKey(pipelineID)).Err(); err != nil {
		return err
	}
	return nil
}
