package plugins

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"

	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
)

// Logger is used for logging formatted messages
type Logger interface {
	// Printf must have the same semantics as log.Printf
	Printf(format string, args ...interface{})
	// Infof writes info level log
	Infof(format string, args ...interface{})
	// Warnf writes warn level log
	Warnf(format string, args ...interface{})
	// Errorf writes error level log
	Errorf(format string, args ...interface{})
	// Fatalf writes error level log and panic
	Fatalf(format string, args ...interface{})
	// WithField adds a key-value pair value that will be printed out
	WithField(key string, value interface{}) *logrus.Entry
}

// LoggerConfig represents logger configuration options
type LoggerConfig struct {
	Path     string        `yaml:"path" default:"log/"`
	InfoAge  time.Duration `yaml:"info_age" default:"72h"`
	WarnAge  time.Duration `yaml:"warn_age" default:"240h"`
	ErrorAge time.Duration `yaml:"error_age" default:"720h"`
}

func NewLogger(config LoggerConfig) (Logger, error) {
	var logPath = filepath.Dir(config.Path)
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		if err = os.Mkdir(logPath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("mkdir %s error:%s", logPath, err)
		}
	}
	infoLog, err := rotatelogs.New(
		logPath+"/info-%Y-%m-%d-%H-%M-%S.log",
		// rotatelogs.WithMaxAge(config.InfoAge),
		rotatelogs.WithRotationCount(3),
		rotatelogs.WithRotationTime(time.Second))
	if err != nil {
		return nil, fmt.Errorf("open info level log writer error:%s", err)
	}
	warnLog, err := rotatelogs.New(
		logPath+"/warn-%Y-%m-%d-%H-%M.log",
		// rotatelogs.WithMaxAge(config.WarnAge),
		rotatelogs.WithRotationCount(3),
		rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open warn level log writer error:%s", err)
	}
	errorLog, err := rotatelogs.New(
		logPath+"/error-%Y-%m-%d-%H.log",
		// rotatelogs.WithMaxAge(config.ErrorAge),
		rotatelogs.WithRotationCount(3),
		rotatelogs.WithRotationTime(time.Hour))
	if err != nil {
		return nil, fmt.Errorf("open error level log writer error:%s", err)
	}
	var logrusLogger = logrus.New()
	formatter := &logrus.JSONFormatter{
		PrettyPrint: false,
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: os.Stdout,
		logrus.InfoLevel:  infoLog,
		logrus.WarnLevel:  warnLog,
		logrus.ErrorLevel: errorLog,
		logrus.FatalLevel: errorLog,
	}, formatter)
	logrusLogger.Hooks[logrus.DebugLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.InfoLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.WarnLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.ErrorLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.FatalLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.PanicLevel] = []logrus.Hook{lfHook}
	return logrusLogger.WithContext(context.Background()), nil
}

type GameLog struct {
	Action  string
	Fields  map[string]interface{}
	Comment string
}
