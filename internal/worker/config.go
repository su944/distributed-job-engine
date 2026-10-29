package worker

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/su944/distributed-job-engine/internal/jobs"
)

type Config struct {
	ID                string
	Capacity          int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	AttemptTimeout    time.Duration
	IOTimeout         time.Duration
	DrainTimeout      time.Duration
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{ID: getenv("WORKER_ID"), Capacity: 4, PollInterval: 250 * time.Millisecond, LeaseDuration: 30 * time.Second, HeartbeatInterval: 10 * time.Second, AttemptTimeout: 5 * time.Minute, IOTimeout: 3 * time.Second, DrainTimeout: 20 * time.Second}
	if c.ID == "" {
		id, err := jobs.UUID()
		if err != nil {
			return c, err
		}
		c.ID = "worker-" + id
	}
	if s := getenv("WORKER_CAPACITY"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return c, errors.New("invalid WORKER_CAPACITY")
		}
		c.Capacity = n
	}
	for name, target := range map[string]*time.Duration{"WORKER_POLL_INTERVAL": &c.PollInterval, "WORKER_LEASE_DURATION": &c.LeaseDuration, "WORKER_HEARTBEAT_INTERVAL": &c.HeartbeatInterval, "WORKER_ATTEMPT_TIMEOUT": &c.AttemptTimeout, "WORKER_IO_TIMEOUT": &c.IOTimeout, "WORKER_DRAIN_TIMEOUT": &c.DrainTimeout} {
		if s := getenv(name); s != "" {
			d, err := time.ParseDuration(s)
			if err != nil {
				return c, fmt.Errorf("invalid %s", name)
			}
			*target = d
		}
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if !jobs.ValidOwner(c.ID) || c.Capacity < 1 || c.Capacity > 64 {
		return errors.New("worker requires valid ID and capacity 1-64")
	}
	for _, d := range []time.Duration{c.PollInterval, c.LeaseDuration, c.HeartbeatInterval, c.AttemptTimeout, c.IOTimeout, c.DrainTimeout} {
		if !jobs.ValidOwnershipDuration(d) {
			return errors.New("worker durations must be positive microseconds through 24h")
		}
	}
	if c.PollInterval < 10*time.Millisecond || c.HeartbeatInterval+c.IOTimeout >= c.LeaseDuration || c.IOTimeout >= c.AttemptTimeout {
		return errors.New("worker heartbeat and I/O budgets must fit inside lease/attempt; poll >=10ms")
	}
	return nil
}
