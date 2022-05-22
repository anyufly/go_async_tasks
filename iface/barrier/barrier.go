package barrier

type IBarrier interface {
	Wait() (bool, error)
	Load(key string) IBarrier
	Create(key string, count int) error
	Delete(key string) error
}
