package pq

import (
	"container/heap"
	"time"
)

type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

type priorityQueue []*Item

func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].Priority == pq[j].Priority {
		return i < j
	}
	return pq[i].Priority < pq[j].Priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(priorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item
}

func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(priorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}

	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

func (pq *priorityQueue) Top() interface{} {
	n := len(*pq)
	if n == 0 {
		return nil
	}
	item := (*pq)[0]
	return item
}

type popSignal struct {
	result chan interface{}
}

type isEmptySignal struct {
	result chan bool
}

type ThreadSafePriorityQueue struct {
	pq          priorityQueue
	pushChan    chan interface{}
	popChan     chan popSignal
	isEmptyChan chan isEmptySignal
	notifyPush  chan struct{}
	close       chan int
}

func NewThreadSafePriorityQueue(size int) *ThreadSafePriorityQueue {
	q := &ThreadSafePriorityQueue{
		pq:          newPriorityQueue(size),
		pushChan:    make(chan interface{}, 1),
		popChan:     make(chan popSignal, 1),
		isEmptyChan: make(chan isEmptySignal, 1),
		close:       make(chan int, 1),
	}
	go func() {
		for {
			select {
			case <-q.close:
				return
			case x := <-q.pushChan:
				heap.Push(&q.pq, x)
				if q.notifyPush != nil {
					q.notifyPush <- struct{}{}
				}
			case sig := <-q.popChan:
				if q.pq.Len() == 0 {
					sig.result <- nil
					continue
				}
				sig.result <- heap.Pop(&q.pq)
			case isEmptySig := <-q.isEmptyChan:
				isEmptySig.result <- q.pq.Len() == 0
			}
		}
	}()

	return q
}

func (q *ThreadSafePriorityQueue) NotifyPush(notify chan struct{}) {
	q.notifyPush = notify
}

func (q *ThreadSafePriorityQueue) PushNotify() chan struct{} {
	return q.notifyPush
}

func (q *ThreadSafePriorityQueue) Push(x interface{}) {
	q.pushChan <- x
}

func (q *ThreadSafePriorityQueue) Pop() *Item {
	var result = make(chan interface{})
	q.popChan <- popSignal{
		result: result,
	}
	o := <-result

	if o == nil {
		return nil
	}
	return o.(*Item)
}

func (q *ThreadSafePriorityQueue) IsEmpty() bool {
	var result = make(chan bool)
	q.isEmptyChan <- isEmptySignal{
		result: result,
	}
	return <-result
}

func (q *ThreadSafePriorityQueue) Close() {
	close(q.close)
}

type ThreadSafeDelayQueue struct {
	pq         priorityQueue
	offerChan  chan interface{}
	notifyChan chan interface{}
	close      chan int
	closed     chan bool
}

func NewThreadSafeDelayQueue(size int) *ThreadSafeDelayQueue {
	q := &ThreadSafeDelayQueue{
		pq:         newPriorityQueue(size),
		offerChan:  make(chan interface{}, 1),
		notifyChan: make(chan interface{}, 1),
		close:      make(chan int, 1),
		closed:     make(chan bool, 1),
	}

	go func() {
		for {
			if q.pq.Len() == 0 {
				select {
				case <-q.close:
					q.closed <- true
					return
				case x := <-q.offerChan:
					heap.Push(&q.pq, x)
				}
			}
			earliest := q.pq.Top()
			delta := earliest.(*Item).Priority - time.Now().UnixNano()

			if delta <= 0 {
			Notify:
				for {
					select {
					case <-q.close:
						q.closed <- true
						return
					case q.notifyChan <- earliest:
						heap.Remove(&q.pq, 0)
						break Notify
					case x := <-q.offerChan:
						heap.Push(&q.pq, x)
						if x.(*Item).Index == 0 {
							// 此时说明插入了更早过期的元素
							break Notify
						}
					}
				}

			} else {
				timer := time.NewTimer(time.Duration(delta))
			Wait:
				for {
					select {
					case <-q.close:
						timer.Stop()
						q.closed <- true
						return
					case x := <-q.offerChan:
						heap.Push(&q.pq, x)
						if x.(*Item).Index == 0 {
							timer.Stop()
							break Wait
						}
					case <-timer.C:
						break Wait
					}
				}

			}
		}
	}()

	return q
}

func (q *ThreadSafeDelayQueue) Notify() chan interface{} {
	return q.notifyChan
}

func (q *ThreadSafeDelayQueue) Offer(x interface{}, expire int64) {
	q.offerChan <- &Item{
		Value:    x,
		Priority: expire,
	}
}

func (q *ThreadSafeDelayQueue) Close() {
	close(q.close)
}

func (q *ThreadSafeDelayQueue) ToSlice() []*Item {
	q.Close()
	<-q.closed
	return q.pq
}
