package test

import (
	"testing"
	"fmt"
	"github.com/Guazi-inc/machinery/example/tasks"
	"github.com/Guazi-inc/machinery/v1"
	"github.com/Guazi-inc/machinery/v1/config"
	"github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
)

var redisDelayedTasksKey = "_delayed_tasks"
var redisDelayedTasksDetail = "_detail"

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
	defer conn.Close()

	task_number1, err := b.GetDelayedTasksNumber()
	if err != nil {
		return
	}

	reply, err := conn.Do(
		"ZRANGEBYSCORE", cnf.DefaultQueue + redisDelayedTasksKey, 0, "+inf")
	msg1, err := redis.ByteSlices(reply, err)

	b.TransferDelayTasks()

	task_number2, err := b.GetDelayedTasksNumber()
	if err != nil {
		return
	}
	msg2 := make([]string, task_number2)
	msg2_uuid, _ := redis.ByteSlices(conn.Do(
		"ZRANGEBYSCORE", cnf.DefaultQueue + redisDelayedTasksKey, 0, "+inf"))
	for i := range msg2_uuid {
		msg2_sig, err := redis.String(conn.Do("get", string(msg2_uuid[i]) + redisDelayedTasksDetail))
		if err != nil {
			return
		}
		if msg2_sig == "" {
			return
		}
		msg2[i] = msg2_sig
	}

	assert.True(t, task_number1 == task_number2, "task number [%d] after transfering is different from " +
		"one [%d] before transfering.", task_number1, task_number2)
	for i := 0; i < task_number1; i++ {
		assert.Equal(t, string(msg1[i]), string(msg2[i]), "signature %s before transfer is different from" +
			"signature %s after transfer", string(msg1[i]), msg2[i])
	}

}
