package actioncable

// A Logger takes the client's chatter. The standard library's *log.Logger
// satisfies it, and so does anything else with a Printf.
type Logger interface {
	Printf(format string, args ...any)
}

// LoggerFunc adapts a function to Logger.
type LoggerFunc func(format string, args ...any)

func (f LoggerFunc) Printf(format string, args ...any) {
	f(format, args...)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}
