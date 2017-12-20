package test

import (
	"testing"
	"fmt"
	"github.com/Guazi-inc/machinery/example/tasks"
	"github.com/Guazi-inc/machinery/v1"
	"github.com/Guazi-inc/machinery/v1/config"
)

func Test_worker(t *testing.T) {
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
	worker := server.NewWorker("machinery_worker", 0)
	worker.Launch()

}
