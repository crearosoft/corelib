package loggermanager

import (
	"os"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("crearosoftlogger")
var format = logging.MustStringFormatter(
	`%{color}%{time:15:04:05} %{shortfile} %{callpath:5} ▶ %{level:.4s} %{id:03x}%{color:reset}`,
)

func init() {

	log.ExtraCalldepth = 1
	backend := logging.NewLogBackend(os.Stderr, "", 0)

	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backend)

	logging.SetBackend(backendLeveled, backendFormatter)

}

// LogDebug logs a message at level Debug on the standard logger.
func LogDebug(args ...interface{}) {
	log.Debug("", args)
}

// LogInfo logs a message at level Info on the standard logger.
func LogInfo(args ...interface{}) {
	log.Info("", args)
}

// LogWarn logs a message at level Warn on the standard logger.
func LogWarn(args ...interface{}) {
	log.Warning("", args)
}

// LogError logs a message at level Error on the standard logger.
func LogError(args ...interface{}) {
	log.Error("", args)
}

// LogPanic logs a message at level Panic on the standard logger.
func LogPanic(args ...interface{}) {
	log.Panic("", args)
}
