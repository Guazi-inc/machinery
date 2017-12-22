package test

import(
	"testing"
	"fmt"

	"github.com/Guazi-inc/machinery/v1"
	"github.com/Guazi-inc/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func Test_GetDelayedTasksNumber(t *testing.T){
	configPath := "/users/bruce/go/src/github.com/Guazi-inc/machinery/example/config.yml"
	cnf, err := config.NewFromYaml(configPath, false)
	if err != nil {
		fmt.Errorf("config from yaml error: %s", err.Error())
	}
	server, err := machinery.NewServer(cnf)
	if err != nil {
		fmt.Errorf("start server error: %s", err.Error())
	}
	var task_number int
	if task_number, err = server.GetBroker().GetDelayedTasksNumber(); err != nil {
		return
	}
	assert.Equal(t, task_number, 5)
}
