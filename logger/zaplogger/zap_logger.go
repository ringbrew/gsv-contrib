package zaplogger

import (
	"github.com/ringbrew/gsv/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

type Option struct {
	Output    []string
	ErrOutput []string
	TimeKey   string
	Skip      int
}

type zapLogger struct {
	level logger.Level
	l     *zap.SugaredLogger
}

func New(opts ...Option) logger.Logger {
	return newZapLogger(opts...)
}

func newZapLogger(opts ...Option) *zapLogger {
	// 自定义时间输出格式
	customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format(time.RFC3339Nano))
	}

	config := zap.NewProductionConfig()
	config.DisableStacktrace = true
	config.Encoding = "json"
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stdout"}
	config.EncoderConfig.TimeKey = "time"
	config.EncoderConfig.EncodeTime = customTimeEncoder
	config.EncoderConfig.CallerKey = "caller"
	config.Level.SetLevel(zapcore.DebugLevel)

	skip := 2

	if len(opts) > 0 {
		opt := opts[0]
		if len(opt.Output) > 0 {
			config.OutputPaths = opt.Output
		}
		if len(opt.ErrOutput) > 0 {
			config.ErrorOutputPaths = opt.ErrOutput
		}
		if opt.TimeKey != "" {
			config.EncoderConfig.TimeKey = opt.TimeKey
		}

		if opt.Skip > 0 {
			skip = opt.Skip
		}
	}

	l, _ := config.Build()

	if skip > 0 {
		l = l.WithOptions(zap.AddCallerSkip(skip))
	}

	zl := &zapLogger{
		l: l.Sugar(),
	}

	return zl
}

func (l *zapLogger) wrap(entry *logger.LogEntry) *zap.SugaredLogger {
	wrapL := l.l

	if entry.TraceId != "" {
		wrapL = wrapL.With(zap.String("trace_id", entry.TraceId))
	}
	if entry.SpanId != "" {
		wrapL = wrapL.With(zap.String("span_id", entry.SpanId))
	}
	if entry.ParentId != "" {
		wrapL = wrapL.With(zap.String("parent_id", entry.ParentId))
	}
	if entry.Extra != nil && len(entry.Extra) > 0 {
		wrapL = wrapL.With(zap.Any("extra", entry.Extra))
	}

	return wrapL
}

func (l *zapLogger) SetLevel(level logger.Level) {
	l.level = level
}

func (l *zapLogger) shouldLog(level logger.Level) bool {
	return l.level <= level
}

func (l *zapLogger) Debug(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelDebug) {
		l.wrap(entry).Debug(entry.Message)
	}
}

func (l *zapLogger) Info(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelInfo) {
		l.wrap(entry).Info(entry.Message)
	}
}

func (l *zapLogger) Warn(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelWarn) {
		l.wrap(entry).Warn(entry.Message)
	}
}

func (l *zapLogger) Error(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelError) {
		l.wrap(entry).Error(entry.Message)
	}
}

func (l *zapLogger) Fatal(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelFatal) {
		l.wrap(entry).Panic(entry.Message)
	}
}

func (l *zapLogger) Close() error {
	return l.l.Sync()
}
