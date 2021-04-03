package hybs

import (
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

type logUserFields map[string]interface{}

func newSysLogger(path string, infoAge, warnAge, errorAge time.Duration) (Logger, error) {
	var logPath = filepath.Dir(path)
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModePerm)
	}
	writerInfo, err := rotatelogs.New(
		logPath+"/info."+"%Yy%mm%dd%Hh%M",
		rotatelogs.WithMaxAge(infoAge),
		rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open info level log writer failed:%s", err)
	}
	writerWarn, err := rotatelogs.New(
		logPath+"/warn."+"%Yy%mm%dd%Hh%M",
		rotatelogs.WithMaxAge(warnAge),
		rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open warn level log writer failed:%s", err)
	}
	writerError, err := rotatelogs.New(
		logPath+"/error."+"%Yy%mm%dd%Hh%M",
		rotatelogs.WithMaxAge(errorAge),
		rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open error level log writer failed:%s", err)
	}
	var logrusLogger = logrus.New()
	formatter := &logrus.JSONFormatter{
		PrettyPrint: false,
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: os.Stdout,
		logrus.InfoLevel:  writerInfo,
		logrus.WarnLevel:  writerWarn,
		logrus.ErrorLevel: writerError,
		logrus.FatalLevel: writerError,
	}, formatter)
	logrusLogger.Hooks[logrus.DebugLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.InfoLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.WarnLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.ErrorLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.FatalLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.PanicLevel] = []logrus.Hook{lfHook}
	return logrusLogger, nil
}

type gameLog struct {
	action   string
	fieldMap map[string]interface{}
	comment  string
}

func newGameLogger(path string, age time.Duration) (Logger, error) {
	var logPath = filepath.Dir(path)
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModePerm)
	}
	writer, err := rotatelogs.New(logPath+"/game."+"%Yy%mm%dd%Hh%M",
		rotatelogs.WithMaxAge(age), rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open game log writer failed:%s", err)
	}
	var logrusLogger = logrus.New()
	formatter := &logrus.JSONFormatter{
		PrettyPrint: false,
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
	}, formatter)
	logrusLogger.Hooks[logrus.DebugLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.InfoLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.WarnLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.ErrorLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.FatalLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.PanicLevel] = []logrus.Hook{lfHook}
	return logrusLogger, nil
}

func newRealtimeLogger(path string, age time.Duration) (Logger, error) {
	var logPath = filepath.Dir(path)
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, os.ModePerm)
	}
	writer, err := rotatelogs.New(logPath+"/realtime."+"%Yy%mm%dd%Hh%M",
		rotatelogs.WithMaxAge(age), rotatelogs.WithRotationTime(time.Minute))
	if err != nil {
		return nil, fmt.Errorf("open game log writer failed:%s", err)
	}
	logrusLogger := logrus.New()
	formatter := &logrus.JSONFormatter{
		PrettyPrint: true,
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
	}, formatter)
	logrusLogger.Hooks[logrus.DebugLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.InfoLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.WarnLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.ErrorLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.FatalLevel] = []logrus.Hook{lfHook}
	logrusLogger.Hooks[logrus.PanicLevel] = []logrus.Hook{lfHook}
	return logrusLogger, nil
}

type loggerConfig struct {
	LogFilePath    string        `yaml:"log_filepath" default:"log/"`
	LogLevel       string        `yaml:"log_level" default:"debug"`
	InfoLogAge     time.Duration `yaml:"info_log_age" default:"72h"`
	WarnLogAge     time.Duration `yaml:"warn_log_age" default:"240h"`
	ErrorLogAge    time.Duration `yaml:"error_log_age" default:"720h"`
	GameLogAge     time.Duration `yaml:"game_log_age" default:"8760h"`
	RealtimeLogAge time.Duration `yaml:"realtime_log_age" default:"24h"`
}
