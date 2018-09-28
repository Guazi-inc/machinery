package brokers

import (
	"github.com/Guazi-inc/machinery/v1/tasks"
	"github.com/garyburd/redigo/redis"
)

const (
	RecordTypePublish RecordType = iota
	RecordTypeProcess
	RecordTypeCancel
)

var taskLoggers []taskLogger

type RecordType int32

type taskLogger func(queueName string, recordType RecordType, signare *tasks.Signature)

func SetTaskLogger(funcs ...taskLogger) {
	taskLoggers = append(taskLoggers, funcs...)
}

// Interface - a common interface for all brokers
type Interface interface {
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(task *tasks.Signature) error
	GetConn() (conn redis.Conn)
	CancelDelayTask(uuid string) error

	CountPendingTasks() (task_number int, err error)
	CountDelayedTasks() (task_number int, err error)
	GetPendingTasks(indexStart, indexEnd int) ([]*tasks.Signature, error)
	GetDelayedTasks(indexStart, indexEnd int) ([]*tasks.Signature, error)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
}
