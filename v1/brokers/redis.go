package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Guazi-inc/machinery/v1/common"
	"github.com/Guazi-inc/machinery/v1/config"
	"github.com/Guazi-inc/machinery/v1/log"
	"github.com/Guazi-inc/machinery/v1/tasks"
	"github.com/garyburd/redigo/redis"
	redsync "gopkg.in/redsync.v1"
)

const (
	redisDelayedQueueSuffix      = "_delayed"
	redisDelayedTaskDetailSuffix = "_detail"
)

func WithDelaySuffix(queue string) string {
	return queue + redisDelayedQueueSuffix
}

func WithDetailSuffix(queue string) string {
	return queue + redisDelayedTaskDetailSuffix
}

// RedisBroker represents a Redis broker
type RedisBroker struct {
	host              string
	password          string
	db                int
	pool              *redis.Pool
	stopReceivingChan chan int
	stopDelayedChan   chan int
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	delayedWG         sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	Broker
	common.RedisConnector
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config, host, password, socketPath string, db int) Interface {
	b := &RedisBroker{Broker: New(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *RedisBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	b.pool = nil
	conn := b.open()
	defer conn.Close()
	defer b.pool.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}

	// Channels and wait groups used to properly close down goroutines
	b.stopReceivingChan = make(chan int)
	b.stopDelayedChan = make(chan int)
	b.receivingWG.Add(1)
	b.delayedWG.Add(1)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte)

	// A receivig goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		defer b.receivingWG.Done()

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			default:
				task, err := b.nextTask(b.cnf.DefaultQueue)
				if err != nil {
					continue
				}

				deliveries <- task
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopDelayedChan:
				return
			default:
				delayedTask, err := b.nextDelayedTask(WithDelaySuffix(b.cnf.DefaultQueue))
				if err != nil {
					continue
				}

				deliveries <- delayedTask
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *RedisBroker) StopConsuming() {
	b.stopConsuming()

	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()

	// Stop the delayed tasks goroutine
	b.stopDelayedChan <- 1
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *RedisBroker) Publish(signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	b.AdjustRoutingKey(signature)

	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			//_, err = conn.Do("ZADD", b.cnf.DefaultQueue+redisDelayedQueueSuffix, score, msg)

			//conn.Send("SET", WithDetailSuffix(signature.UUID), msg)
			conn.Send("HSET", WithDetailSuffix(b.cnf.DefaultQueue), signature.UUID, msg)
			conn.Send("ZADD", WithDelaySuffix(b.cnf.DefaultQueue), score, signature.UUID)
			conn.Flush()
			if _, err = conn.Receive(); err != nil {
				return err
			}
		}
	} else {
		if _, err = conn.Do("RPUSH", signature.RoutingKey, msg); err != nil {
			return err
		}

	}

	b.SaveRecord(RecordTypePublish, signature)
	return nil
}

func (b *RedisBroker) SaveRecord(recordType RecordType, signare *tasks.Signature) {
	for _, f := range saveRecordFuncs {
		f(b.cnf.DefaultQueue, recordType, signare)
	}
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *RedisBroker) GetPendingTasks(indexStart, indexEnd int) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if indexStart < 0 || indexEnd < indexStart {
		indexStart = 0
		indexEnd = 10
	}
	bytes, err := conn.Do("LRANGE", b.cnf.DefaultQueue, 0, 10)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(bytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		sig := new(tasks.Signature)
		if err := json.Unmarshal(result, sig); err != nil {
			return nil, err
		}
		taskSignatures[i] = sig
	}
	return taskSignatures, nil
}

func (b *RedisBroker) GetDelayedTasks(indexStart, indexEnd int) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if indexStart < 0 || indexEnd < indexStart {
		indexStart = 0
		indexEnd = 10
	}
	bytes, err := conn.Do("ZRANGE", WithDelaySuffix(b.cnf.DefaultQueue), 0, 10)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(bytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		sig := new(tasks.Signature)
		//bytes, err = conn.Do("GET", WithDetailSuffix(string(result)))
		bytes, err = conn.Do("HGET", WithDetailSuffix(b.cnf.DefaultQueue), string(result))
		if err != nil {
			return nil, err
		}
		detail, err := redis.Bytes(bytes, err)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(detail, sig); err != nil {
			return nil, err
		}
		taskSignatures[i] = sig
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *RedisBroker) consume(deliveries <-chan []byte, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error, concurrency*2)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.Broker.stopChan:
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *RedisBroker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	sig := new(tasks.Signature)
	if err := json.Unmarshal(delivery, sig); err != nil {
		return err
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(sig.Name) {
		conn := b.open()
		defer conn.Close()

		conn.Do("RPUSH", b.cnf.DefaultQueue, delivery)
		return nil
	}

	log.INFO.Printf("Received new message: %s", delivery)

	if err := taskProcessor.Process(sig); err != nil {
		return err
	}
	b.SaveRecord(RecordTypeProcess, sig)
	return nil
}

// nextTask pops next available task from the default queue
func (b *RedisBroker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, 1))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/garyburd/redigo/blob/master/redis/zpop_example_test.go
func (b *RedisBroker) nextDelayedTask(key string) (result []byte, err error) {

	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items     [][]byte
		msg_byte  [][]byte
		reply     interface{}
		msg_delay interface{}
	)

	for {
		// Space out queries to ZSET to 20ms intervals so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		<-time.After(20 * time.Millisecond)
		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do("ZRANGEBYSCORE", key, 0, now, "LIMIT", 0, 1))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		//if msg_delay, err = conn.Do("GET", WithDetailSuffix(string(items[0]))); err != nil {
		if msg_delay, err = conn.Do("HGET", WithDetailSuffix(b.cnf.DefaultQueue), string(items[0])); err != nil {
			return
		}
		if msg_delay == nil {
			err = fmt.Errorf("signature message for %s is nil: %s", string(items[0]))
			return
		}

		msg_delay_slice := make([]interface{}, 1)
		msg_delay_slice[0] = msg_delay

		if msg_byte, err = redis.ByteSlices(msg_delay_slice, err); err != nil {
			return
		}

		conn.Send("MULTI")
		conn.Send("ZREM", key, items[0])
		//conn.Send("DEL", WithDetailSuffix(string(items[0])))
		conn.Send("HDEL", WithDetailSuffix(b.cnf.DefaultQueue), string(items[0]))
		if reply, err = conn.Do("EXEC"); err != nil {
			return
		}

		//删除失败，可能已经被消费
		if reply.([]interface{})[0] == int64(0) {
			log.INFO.Printf("delayed task: %s, may already consumed", items[0])
			err = errors.New(fmt.Sprintf("delayed task: %s, may already consumed", items[0]))
			return
		}

		if reply != nil {
			result = msg_byte[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *RedisBroker) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db)
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	return b.pool.Get()
}

//transfer delay tasks to suit updated code in which ETA of tasks can be modified
func (b *RedisBroker) TransferDelayTask(queue, newQueue string, start, end int) (errRet error) {
	if start == 0 && end == 0 {
		end = -1
	} else if start < 0 || end <= start {
		return errors.New("invalid params")
	}

	conn := b.open()
	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if errRet != nil {
			log.ERROR.Println(errRet)
			conn.Do("DISCARD")
		}
		conn.Close()
	}()

	for {
		// Space out queries to ZSET to 20ms intervals so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		<-time.After(20 * time.Millisecond)

		if _, err := conn.Do("WATCH", queue); err != nil {
			return err
		}

		// https://redis.io/commands/zrangebyscore
		results, err := redis.ByteSlices(conn.Do(
			"ZRANGE", queue, start, end,
		))
		if err != nil {
			return err
		}
		log.INFO.Printf("[TransferDelayTasks] %d results", len(results))

		conn.Send("MULTI")
		for i := range results {
			sig := new(tasks.Signature)
			if err := json.Unmarshal(results[i], sig); err != nil {
				return err
			}
			log.INFO.Printf("[%d] %+v", i, sig)
			score := sig.ETA.UnixNano()
			//if err = conn.Send("SET", WithDetailSuffix(sig.UUID), results[i]); err != nil {
			if err = conn.Send("HSET", WithDetailSuffix(newQueue), sig.UUID, results[i]); err != nil {
				return err
			}
			if err = conn.Send("ZADD", WithDelaySuffix(newQueue), score, sig.UUID); err != nil {
				return err
			}
		}

		reply, err := conn.Do("EXEC")
		if err != nil {
			return err
		}
		if reply != nil {
			break
		}
	}
	return nil
}

//transfer delay tasks to suit updated code in which ETA of tasks can be modified
func (b *RedisBroker) TransferTask(queue, newQueue string, start, end int) (errRet error) {
	if start == 0 && end == 0 {
		end = -1
	} else if start < 0 || end <= start {
		return errors.New("invalid params")
	}

	conn := b.open()
	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if errRet != nil {
			log.ERROR.Println(errRet)
			conn.Do("DISCARD")
		}
		conn.Close()
	}()

	for {
		// Space out queries to ZSET to 20ms intervals so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		<-time.After(20 * time.Millisecond)

		if _, err := conn.Do("WATCH", queue); err != nil {
			return err
		}

		// https://redis.io/commands/zrangebyscore
		results, err := redis.ByteSlices(conn.Do(
			"LRANGE", queue, start, end,
		))
		if err != nil {
			return err
		}
		log.INFO.Printf("[TransferTasks] %d results", len(results))

		conn.Send("MULTI")
		for i := range results {
			sig := new(tasks.Signature)
			if err := json.Unmarshal(results[i], sig); err != nil {
				return err
			}
			log.INFO.Printf("[%d] %+v", i, sig)
			if err = conn.Send("RPUSH", newQueue, results[i]); err != nil {
				return err
			}
		}

		reply, err := conn.Do("EXEC")
		if err != nil {
			return err
		}
		if reply != nil {
			break
		}
	}
	return nil
}

//get connection to redis for unit test
func (b *RedisBroker) GetConn() (conn redis.Conn) {
	conn = b.open()
	return
}

func (b *RedisBroker) CountDelayedTasks() (int, error) {
	conn := b.open()
	defer conn.Close()

	reply, err := conn.Do("ZCARD", WithDelaySuffix(b.cnf.DefaultQueue))
	if err != nil {
		return 0, err
	}

	return redis.Int(reply, err)
}

func (b *RedisBroker) CountPendingTasks() (int, error) {
	conn := b.open()
	defer conn.Close()

	reply, err := conn.Do("LLEN", b.cnf.DefaultQueue)
	if err != nil {
		return 0, err
	}

	return redis.Int(reply, err)
}
