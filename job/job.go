package job

type Job interface {
	Handle() error
}
