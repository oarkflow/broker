package logger

import (
	"os"

	"github.com/oarkflow/zlog"
)

var (
	Log = zlog.New(zlog.Options{
		Level:     zlog.DebugLevel,
		Sink:      zlog.NewWriterSink(os.Stderr, zlog.NewConsoleEncoder(), zlog.TraceLevel),
		AddCaller: true,
	})
	StdLog = Log.StandardLogger(zlog.InfoLevel)
)

// Logf is a Printf-style adapter compatible with broker logf func(string, ...any).
func Logf(format string, args ...any) {
	StdLog.Printf(format, args...)
}
