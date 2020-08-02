package loggermanager

import (
	"encoding/json"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger
var sugar *zap.SugaredLogger

// Init  Init Logger
// maxBackupFileSize,  megabytes
// maxAgeForBackupFile,  days
func Init(fileName string, maxBackupCnt, maxBackupFileSize, maxAgeForBackupFileInDays int, loglevel zapcore.Level) {
	os.MkdirAll(filepath.Dir(fileName), os.ModePerm)

	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    maxBackupFileSize, // megabytes
		MaxBackups: maxBackupCnt,
		MaxAge:     maxAgeForBackupFileInDays, // days
	})

	// zap.AddStacktrace(
	rawJSON := []byte(`{
	  "level": "debug",
	  "encoding": "json",
	  "outputPaths": ["stdout", "./data/logs"],
	  "errorOutputPaths": ["stderr"],
	  "initialFields": {"foo": "bar"},
	  "disableCaller":false,
	  "encoderConfig": {
	    "messageKey": "m",
	    "callerKey": "c",
	    "callerEncode": 0,
	    "timeKey": "t",
		"levelKey": "l",
	    "levelEncoder": "lowercase"
	  }
	}`)

	var cfg zap.Config
	json.Unmarshal(rawJSON, &cfg)
	core := zapcore.NewCore(
		//enc, //
		zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
		w,
		loglevel,
	)

	logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	defer logger.Sync()
	sugar = logger.Sugar()

}
