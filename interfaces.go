package tsqlx

import (
	"time"
)

type ITracer interface {
	TraceDependency(
		spanId string,
		dependencyType string,
		serviceName string,
		commandName string,
		success bool,
		startTimestamp time.Time,
		eventTimestamp time.Time,
		fields map[string]string,
	)
}
