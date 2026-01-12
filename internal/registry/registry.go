package registry

import (
	"fmt"
	"sync"

	"github.com/danish-a1/goqueue/job"
)

type Constructor func() job.Job

var (
	mu           sync.RWMutex
	constructors = make(map[string]Constructor)
)

func Register(name string, constructor Constructor) {
	mu.Lock()
	defer mu.Unlock()

	if name == "" {
		panic("job name cannot be empty")
	}
	if _, exists := constructors[name]; exists {
		panic(fmt.Sprintf("job type %q already registered", name))
	}
	constructors[name] = constructor
}

// Get returns a constructor for the given job name
func GetFromRegistery(name string) (Constructor, bool) {
	mu.RLock()
	defer mu.RUnlock()

	constructor, ok := constructors[name]
	return constructor, ok
}
