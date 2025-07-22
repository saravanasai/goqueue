package memory

import (
	"strconv"
	"sync/atomic"
)

var idCounter uint64

func generateID() string {
	return strconv.FormatUint(atomic.AddUint64(&idCounter, 1), 10)
}
