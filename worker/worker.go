package worker

import (
	"errors"
	"os"
	"runtime"
	"time"

	"github.com/fakerjeff/go_async_tasks/config"

	"github.com/fakerjeff/go_async_tasks/util"

	mapSet "github.com/deckarep/golang-set"
	"github.com/fakerjeff/go_async_tasks/iface/broker"
	"github.com/fakerjeff/go_async_tasks/iface/ident"
	"github.com/fakerjeff/go_async_tasks/iface/log"
	"github.com/fakerjeff/go_async_tasks/iface/task"
	"github.com/fakerjeff/go_async_tasks/logger"
	"github.com/fakerjeff/go_async_tasks/util/pq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultWorkQueueSize  = 25
	DefaultReConnInterval = 5 * time.Second
)

var (
	taskNotFound = errors.New("can't found task in broker")
)

type Worker struct {
	ID               string                      //WorkerID
	logger           log.Logger                  // 日志记录器
	broker           broker.IBroker              // broker
	concurrency      int                         // worker 协程数
	queuePrefetch    int                         //队列预取
	consumeWhiteList mapSet.Set                  //消费的队列名称集合
	consumers        map[string]*consumerProcess // 消费者Map
	workers          map[string]*workProcess     //工作队列
	workQueue        *pq.ThreadSafePriorityQueue // 工作队列
	dlxWorkQueue     *pq.ThreadSafePriorityQueue // 死信工作队列
	reConnInterval   time.Duration               //重连时间间隔
	IsDlxWorker      bool                        //是否为死信队列消费Worker
	dlxTaskProcessor string                      // 死信队列处理器名称
	close            chan int                    // close channel
}

func NewWorker(b broker.IBroker, concurrency int, queueNames []string, reConnInterval time.Duration, dlxWorker bool) *Worker {
	if concurrency == 0 {
		concurrency = 2 * runtime.NumCPU()
	}
	consumerWhiteList := mapSet.NewSet()
	for _, queueName := range queueNames {
		consumerWhiteList.Add(queueName)
	}
	idMaker := ident.NewUUID4Maker()

	if reConnInterval <= time.Duration(0) {
		reConnInterval = DefaultReConnInterval
	}
	workQueue := pq.NewThreadSafePriorityQueue(DefaultWorkQueueSize)
	workQueue.NotifyPush(make(chan struct{}, 1))

	worker := &Worker{
		ID:          idMaker.NewID(),
		broker:      b,
		concurrency: concurrency,
		logger: logger.NewDefaultLogger("go_async_tasks.worker.Worker", logger.Config{
			LogLevel: zapcore.InfoLevel,
			Writer:   os.Stdout,
			Options:  []zap.Option{zap.AddCaller(), zap.AddCallerSkip(1)},
		}),
		consumeWhiteList: consumerWhiteList,
		close:            make(chan int, 1),
		consumers:        make(map[string]*consumerProcess),
		workers:          make(map[string]*workProcess),
		queuePrefetch:    concurrency * 2,
		workQueue:        workQueue,
		reConnInterval:   reConnInterval,
		IsDlxWorker:      dlxWorker,
	}

	if dlxWorker {
		dlxWorkerQueue := pq.NewThreadSafePriorityQueue(DefaultWorkQueueSize)
		dlxWorkerQueue.NotifyPush(make(chan struct{}, 1))
		worker.dlxWorkQueue = dlxWorkerQueue
	}
	return worker
}

func (w *Worker) Start() {
	var consumerErrChan = make(chan *consumerError, 1)
	for queueName := range w.consumeWhiteList.Iter() {
		if w.IsDlxWorker {
			// 添加死信队列消费者
			w.addConsumer(util.XQName(queueName.(string)), true, consumerErrChan)
		} else {
			// 添加普通、延时队列消费者
			w.addConsumer(util.QName(queueName.(string)), false, consumerErrChan)
			w.addConsumer(util.DQName(queueName.(string)), false, consumerErrChan)
		}
	}

	var workerErrChan = make(chan *workerError, 1)
	for i := 0; i < w.concurrency; i++ {
		if w.IsDlxWorker {
			//w.addDlxWorker()
		} else {
			w.addWorker(nil, workerErrChan)
		}
	}
Process:
	for {
		select {
		case <-w.close:
			w.Close()
			break Process
		case consumerErr := <-consumerErrChan:
			w.logger.Error("encountered a consumer error", "consumerId", consumerErr.consumerId, "err", consumerErr.err)
			w.Close()
			break Process
		case workerErr := <-workerErrChan:
			w.logger.Error("encountered a worker error", "workerId", workerErr.workerId, "err", workerErr.err)
			w.Close()
			break Process
		}
	}
}

type consumerError struct {
	consumerId string
	err        error
}

type workerError struct {
	workerId string
	err      error
}

func (w *Worker) addWorker(task task.ITask, errChan chan *workerError) {
	workerId := config.IDMaker().NewID()
	l := w.logger.FromName("worker", zap.String("id", workerId))
	process := newWorkProcess(workerId, w.broker, w.workQueue, w.consumers, l, task)
	w.workers[workerId] = process

	go func() {
		err := process.Process()
		if err != nil {
			errChan <- &workerError{
				workerId: workerId,
				err:      err,
			}
		}
	}()
}

func (w *Worker) addConsumer(queueName string, dlx bool, errChan chan *consumerError) {
	if _, ok := w.consumers[queueName]; ok {
		return
	}
	prefetch := w.queuePrefetch
	consumerId := config.IDMaker().NewID()
	workQueue := w.workQueue
	if dlx {
		workQueue = w.dlxWorkQueue
	}
	l := w.logger.FromName("consumer", zap.String("id", consumerId), zap.String("queueName", queueName))
	process := newConsumerProcess(consumerId, queueName, prefetch, w.broker, workQueue, l, w.reConnInterval, dlx)
	w.consumers[queueName] = process

	go func() {
		err := process.Process()
		if err != nil {
			errChan <- &consumerError{
				consumerId: consumerId,
				err:        err,
			}
		}
	}()
}

func (w *Worker) WithLogger(logger log.Logger) *Worker {
	w.logger = logger
	return w
}

func (w *Worker) Close() {
	close(w.close)

	for _, consumer := range w.consumers {
		consumer.Close()
	}

	for _, worker := range w.workers {
		worker.Close()
	}
	w.workQueue.Close()
	w.broker.Close()
}
