package worker

type IWorker interface {
	Start()
	Close()
}

type IWorkerProcess interface {
	Process() error
	Close()
}
