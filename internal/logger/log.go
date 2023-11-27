package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

func Init(verbosity string, filepath string) {
	zerolog.TimeFieldFormat = time.RFC3339
	if filepath != "" {
		runLogFile, err := os.OpenFile(
			cleanPath(filepath),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0664,
		)
		if err != nil {
			panic(err)
		}
		fileLogger := zerolog.New(runLogFile).With().Logger()
		writers := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout}, fileLogger)
		log.Logger = log.Output(writers)
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	var level zerolog.Level
	switch verbosity {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	default:
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
}

func ExtJSONString(filter interface{}) string {
	bytes, err := bson.MarshalExtJSON(filter, true, true)
	if err != nil {
		log.Fatal().Err(err)
	}
	return string(bytes)
}

func cleanPath(path string) string {
	cleaned, _ := strings.CutSuffix(path, "/")
	return cleaned
}
