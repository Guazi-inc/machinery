package brokers

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Guazi-inc/machinery/v1/tasks"
	"github.com/garyburd/redigo/redis"
)

// EagerBroker represents an "eager" in-memory broker
type EagerBroker struct {
	worker TaskProcessor
	Broker
}

//// NewEagerBroker creates new EagerBroker instance
//func NewEagerBroker() Interface {
//	return new(EagerBroker)
//}

// EagerMode interface with methods specific for this broker
type EagerMode interface {
	AssignWorker(p TaskProcessor)
}

// StartConsuming enters a loop and waits for incoming messages
func (eagerBroker *EagerBroker) StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error) {
	return true, nil
}

// StopConsuming quits the loop
func (eagerBroker *EagerBroker) StopConsuming() {
	// do nothing
}

// Publish places a new message on the default queue
func (eagerBroker *EagerBroker) Publish(task *tasks.Signature) error {
	if eagerBroker.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}

	// faking the behavior to marshal input into json
	// and unmarshal it back
	message, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	signature := new(tasks.Signature)
	err = json.Unmarshal(message, &signature)
	if err != nil {
		return fmt.Errorf("JSON unmarshal error: %s", err)
	}

	// blocking call to the task directly
	return eagerBroker.worker.Process(signature)
}

// AssignWorker assigns a worker to the eager broker
func (eagerBroker *EagerBroker) AssignWorker(w TaskProcessor) {
	eagerBroker.worker = w
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (eagerBroker *EagerBroker) GetPendingTasks(_ int, _ int) ([]*tasks.Signature, error) {
	return []*tasks.Signature{}, errors.New("Not implemented")
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (eagerBroker *EagerBroker) GetDelayedTasks(_ int, _ int) ([]*tasks.Signature, error) {
	return nil, nil
}

func (eagerBroker *EagerBroker) TransferDelayTasks(_, _ string) error {
	return nil
}

func (eagerBroker *EagerBroker) GetConn() redis.Conn {
	return nil
}

func (eagerBroker *EagerBroker) CountDelayedTasks() (int, error) {
	return 0, nil
}

func (eagerBroker *EagerBroker) CountPendingTasks() (int, error) {
	return 0, nil
}
