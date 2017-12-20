package test

import (
	"testing"
	"fmt"
	"github.com/Guazi-inc/machinery/example/tasks"
	"github.com/Guazi-inc/machinery/v1"
	"github.com/Guazi-inc/machinery/v1/config"
	"github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
	"time"
)

var redisDelayedTasksKey = "_delayed_tasks"

func Test_TransferDelayTasks(t *testing.T) {
	configPath := "/users/bruce/go/src/github.com/Guazi-inc/machinery/example/config.yml"
	cnf, err := config.NewFromYaml(configPath, false)
	if err != nil {
		fmt.Errorf("config from yaml error: %s", err.Error())
	}
	server, err := machinery.NewServer(cnf)
	if err != nil {
		fmt.Errorf("start server error: %s", err.Error())
	}

	tasks := map[string]interface{}{
		"add":               exampletasks.Add,
		"multiply":          exampletasks.Multiply,
		"panic_task":        exampletasks.PanicTask,
		"long_running_task": exampletasks.LongRunningTask,
	}
	err = server.RegisterTasks(tasks)
	if err != nil {
		fmt.Errorf("start server error: %s", err.Error())
	}

	b := server.GetBroker()
	conn := b.GetConn()

	reply1, err := conn.Do("zcard", cnf.DefaultQueue + redisDelayedTasksKey)
	if err != nil {
		fmt.Errorf("get task number error: %s", err.Error())
	}
	task_number1, err1 := reply1.(int)
	if !err1 {
		fmt.Errorf("task number type error: %s", reply1)
	}

	now := time.Now().UTC().UnixNano()
	msg1 := make([][]byte, task_number1)
	msg1, _ = redis.ByteSlices(conn.Do(
		"ZRANGEBYSCORE", cnf.DefaultQueue + redisDelayedTasksKey, 0, now, "LIMIT", 0,  task_number1,))

	b.TransferDelayTasks()

	reply2, err := conn.Do("zcard", cnf.DefaultQueue + redisDelayedTasksKey)
	if err != nil {
		fmt.Errorf("get task number error: %s", err.Error())
	}
	task_number2, err2 := reply2.(int)
	if !err2 {
		fmt.Errorf("task number type error: %s", reply1)
	}

	msg2 := make([][]byte, task_number1)
	msg2_uuid, _ := redis.ByteSlices(conn.Do(
		"ZRANGEBYSCORE", cnf.DefaultQueue + redisDelayedTasksKey, 0, now, "LIMIT", 0,  task_number2,))
	for i := range msg2_uuid {
		msg2_sig, _ := redis.ByteSlices(conn.Do("get", msg2_uuid[i]))
		assert.Equal(t, len(msg2_sig), 1, "actual lenght for %s is %d", msg2_uuid[i], len(msg2_sig))
		msg2[i] = msg2_sig[0]
	}

	assert.True(t, task_number1 == task_number2, "task number [%d] after transfering is different from " +
		"one [%d] before transfering.", task_number1, task_number2)
	for i := 0; i < task_number1; i++ {
		assert.Equal(t, string(msg1[i]), string(msg2[i]), "signature %s before transfer is different from" +
			"signature %s after transfer", string(msg1[i]), string(msg2[i]))
	}

}
