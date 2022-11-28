package filelogger

import (
	"encoding/json"
	"fmt"
	"github.com/ringbrew/gsv/logger"
	"log"
	"os"
	"strings"
	"time"
)

type Option struct {
	Path string
}

const logFormat = "[%s] [%s] [%s]"

type fileLogger struct {
	level logger.Level
	file  *os.File
}

func New(opts ...Option) logger.Logger {
	return newFileLogger(opts...)
}

func newFileLogger(opts ...Option) *fileLogger {
	fp := "log.txt"

	if len(opts) > 0 && opts[0].Path != "" {
		fp = opts[0].Path
	}

	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err.Error())
	}

	return &fileLogger{
		file: f,
	}
}

func (l *fileLogger) SetLevel(level logger.Level) {
	l.level = level
}

func (l *fileLogger) GetLevel() logger.Level {
	return l.level
}

func (l *fileLogger) shouldLog(level logger.Level) bool {
	return l.level <= level
}

func (l *fileLogger) Debug(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelDebug) {
		l.writeEntry(entry, logger.LevelDebug)
	}
}

func (l *fileLogger) Info(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelInfo) {
		l.writeEntry(entry, logger.LevelInfo)
	}
}

func (l *fileLogger) Warn(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelWarn) {
		l.writeEntry(entry, logger.LevelWarn)
	}
}

func (l *fileLogger) Error(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelError) {
		l.writeEntry(entry, logger.LevelError)
	}
}

func (l *fileLogger) Fatal(entry *logger.LogEntry) {
	if l.shouldLog(logger.LevelFatal) {
		l.writeEntry(entry, logger.LevelFatal)
	}
}

func (l *fileLogger) writeEntry(entry *logger.LogEntry, level logger.Level) {
	sb := strings.Builder{}

	msg := fmt.Sprintf(logFormat, time.Now(), level.String(), entry.Message)

	sb.WriteString(msg)
	if entry.TraceId != "" {
		sb.WriteString(fmt.Sprintf("-[trace:%s]-[span:%s]", entry.TraceId, entry.SpanId))
	}

	if entry.Extra != nil && len(entry.Extra) > 0 {
		extraData, _ := json.Marshal(entry.Extra)
		sb.WriteString(fmt.Sprintf("-[extra-%s]", string(extraData)))
	}

	l.file.WriteString(sb.String())
}

func (l *fileLogger) write(data string) {
	if _, err := l.file.WriteString(data); err != nil {
		log.Println("log error:", err.Error())
	}
	if _, err := l.file.WriteString("\n"); err != nil {
		log.Println("log error:", err.Error())
	}
	return
}

func (l *fileLogger) Close() error {
	return l.file.Close()
}
