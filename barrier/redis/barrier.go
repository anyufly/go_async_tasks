package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/fakerjeff/go_async_tasks/iface/barrier"
	"github.com/go-redis/redis/v8"
)

var barrierDone = errors.New("barrier done")

type Barrier struct {
	key   string
	count uint64
	rdb   *redis.Client
}

func NewBarrier(rdb *redis.Client) *Barrier {
	b := &Barrier{
		rdb: rdb,
	}
	return b
}

func (c *Barrier) Load(key string) barrier.IBarrier {
	b := &Barrier{
		key: fmt.Sprintf("barrier:%s", key),
		rdb: c.rdb,
	}
	return b
}

func (c *Barrier) Create(key string, count int) error {
	if err := c.rdb.SetNX(context.Background(), fmt.Sprintf("barrier:%s", key), count, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Barrier) doDecr(tx *redis.Tx) error {
	ctx := context.Background()
	value, err := c.rdb.Get(ctx, c.key).Uint64()
	if err != nil {
		return err
	}
	value -= 1

	if value == 0 {
		return barrierDone
	}
	_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if e := pipe.Set(ctx, c.key, value, 0).Err(); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Barrier) Delete(key string) error {
	if err := c.rdb.Del(context.Background(), fmt.Sprintf("barrier:%s", key)).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Barrier) decr() error {
	ctx := context.Background()
	for {
		err := c.rdb.Watch(ctx, c.doDecr, c.key)
		if err == nil {
			return nil
		}
		if err == redis.TxFailedErr {
			continue
		}
		return err
	}
}

func (c *Barrier) Wait() (bool, error) {
	err := c.decr()
	if err == nil {
		return true, nil
	} else if err == barrierDone {
		ctx := context.Background()
		c.rdb.Del(ctx, c.key)
		return false, nil
	} else {
		return false, err
	}
}
