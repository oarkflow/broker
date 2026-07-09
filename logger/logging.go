package logger

import (
	"os"

	"github.com/oarkflow/zlog"
)

var (
	Log = zlog.New(zlog.Options{
		Level:     zlog.DebugLevel,
		Sink:      zlog.NewWriterSink(os.Stderr, zlog.NewConsoleEncoder(), zlog.TraceLevel),
		AddCaller: false,
	})
	StdLog = Log.StandardLogger(zlog.InfoLevel)
)
