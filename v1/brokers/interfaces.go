package brokers

import (
	"github.com/Guazi-inc/machinery/v1/tasks"
	"github.com/garyburd/redigo/redis"
)

// Interface - a common interface for all brokers
type Interface interface {
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	TransferDelayTasks(newQueueName string) (err error)
	GetConn() (conn redis.Conn)
	GetDelayedTasksNumber() (task_number int, err error)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
}
