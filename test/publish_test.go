package test

import (
	"testing"
	"fmt"

	"github.com/Guazi-inc/machinery/v1"
	"github.com/Guazi-inc/machinery/v1/config"
	"github.com/Guazi-inc/machinery/v1/log"
	"github.com/Guazi-inc/machinery/v1/tasks"
	"time"
)

func Test_publish(t *testing.T){
	configPath := "/users/bruce/go/src/github.com/Guazi-inc/machinery/example/config.yml"
	cnf, err := config.NewFromYaml(configPath, false)
	if err != nil {
		fmt.Errorf("config from yaml error: %s", err.Error())
	}
	server, err := machinery.NewServer(cnf)
	if err != nil {
		fmt.Errorf("start server error: %s", err.Error())
	}

	var (
		addTask0, addTask1, addTask2 tasks.Signature
		multiplyTask0, multiplyTask1 tasks.Signature
	)
	eta0 := time.Now().UTC().Add(time.Second * 120)
	eta1 := time.Now().UTC().Add(time.Second * 130)
	eta2 := time.Now().UTC().Add(time.Second * 140)
	eta3 := time.Now().UTC().Add(time.Second * 150)
	eta4 := time.Now().UTC().Add(time.Second * 160)
	var initTasks = func() {
		addTask0 = tasks.Signature{
			UUID: "zzl_add0",
			Name: "add",
			ETA:  &eta0,
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 1,
				},
				{
					Type:  "int64",
					Value: 1,
				},
			},
		}

		addTask1 = tasks.Signature{
			UUID: "zzl_add1",
			Name: "add",
			ETA:  &eta1,
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 2,
				},
				{
					Type:  "int64",
					Value: 2,
				},
			},
		}

		addTask2 = tasks.Signature{
			Name: "add",
			ETA:  &eta2,
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 5,
				},
				{
					Type:  "int64",
					Value: 6,
				},
			},
		}

		multiplyTask0 = tasks.Signature{
			Name: "multiply",
			ETA:  &eta3,
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 4,
				},
			},
		}

		multiplyTask1 = tasks.Signature{
			Name: "multiply",
			ETA:  &eta4,

		}
	}
	initTasks()
	log.INFO.Println("Single task:")
	_, err = server.SendTask(&addTask0)
	_, err = server.SendTask(&addTask1)
	_, err = server.SendTask(&addTask2)
	_, err = server.SendTask(&multiplyTask0)
	_, err = server.SendTask(&multiplyTask1)
	if err != nil {
		fmt.Errorf("send tasks error: %s", err.Error())
	}
}
