package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/Guazi-inc/machinery/v1/config"
)

func TestRedisMemcache(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if redisURL == "" || memcacheURL == "" {
		return
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("memcache://%v", memcacheURL),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
