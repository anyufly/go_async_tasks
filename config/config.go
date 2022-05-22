package config

import (
	"sync"

	"github.com/fakerjeff/go_async_tasks/iface/barrier"

	"github.com/fakerjeff/go_async_tasks/iface/backend"
	"github.com/fakerjeff/go_async_tasks/iface/ident"
)

var idMaker ident.IDMaker
var idMakerOnce sync.Once

func DefaultIDMaker() ident.IDMaker {
	return ident.NewUUID4Maker()
}

func IDMaker() ident.IDMaker {
	if idMaker == nil {
		SetIDMaker(DefaultIDMaker())
	}
	return idMaker
}

func SetIDMaker(maker ident.IDMaker) {
	idMakerOnce.Do(func() {
		idMaker = maker
	})
}

var back backend.IBackend
var backendOnce sync.Once

func Backend() backend.IBackend {
	return back
}

func SetBackend(backend backend.IBackend) {
	backendOnce.Do(func() {
		back = backend
	})
}

var groupBarrier barrier.IBarrier
var groupBarrierOnce sync.Once

func Barrier() barrier.IBarrier {
	return groupBarrier
}

func SetGroupBarrier(barrier barrier.IBarrier) {
	groupBarrierOnce.Do(func() {
		groupBarrier = barrier
	})
}
