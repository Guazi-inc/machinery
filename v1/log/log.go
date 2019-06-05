package log

import (
	"fmt"

	"github.com/RichardKnop/logging"
)

var (
	logger = logging.New(nil, nil, new(logging.ColouredFormatter))

	// DEBUG ...
	DEBUG = logger[logging.DEBUG]
	// INFO ...
	INFO = logger[logging.INFO]
	// WARNING ...
	WARNING = logger[logging.WARNING]
	// ERROR ...
	ERROR = logger[logging.ERROR]
	// FATAL ...
	FATAL = logger[logging.FATAL]
)

// Set sets a custom logger
func Set(l logging.LoggerInterface) {
	DEBUG = l
	INFO = l
	WARNING = l
	ERROR = l
	FATAL = l
}

func ToString(in interface{}) string {
	return fmt.Sprintf("%+v", in)
}

func Truncate(msg string) string {
	msgBytes := []byte(msg)
	if len(msgBytes) > 500 {
		return string(msgBytes[:500]) + "..."
	}
	return msg
}
