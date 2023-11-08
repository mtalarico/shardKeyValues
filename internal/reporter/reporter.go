package reporter

import (
	"context"
	"os"
	"sk/internal/logger"
	"sk/internal/worker"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

type Reporter struct {
	queue     chan report
	pool      *worker.Pool
	file      *os.File
	jsonArray bool
}

type report struct {
	details bson.D
	str     string
}

func NewReporter(filePath string, jsonArary bool) Reporter {
	if info, _ := os.Stat(filePath); info != nil {
		os.Rename(filePath, filePath+"."+time.Now().Local().Format(time.RFC3339)+".bak")
	}
	inputFile, err := os.Create(filePath)
	if err != nil {
		log.Fatal().Err(err)
	}

	r := Reporter{
		queue:     make(chan report),
		file:      inputFile,
		jsonArray: jsonArary,
	}

	logger := log.With().Str("c", "reporter").Logger()
	pool := worker.NewWorkerPool(logger, 1, "reporterWorkers")

	pool.Start(func(iCtx context.Context, iLogger zerolog.Logger) {
		r.processReports(iCtx, iLogger)
	})
	r.pool = &pool
	return r
}

func (r *Reporter) Done(ctx context.Context, logger zerolog.Logger) {
	logger.Debug().Msg("closing reporter queue and waiting for reporters to finish")
	close(r.queue)
	r.pool.Done()
}

func (r *Reporter) ReportString(value string) {
	rep := report{str: value}
	r.queue <- rep
}

func (r *Reporter) ReportValue(value bson.D) {
	rep := report{details: value}
	r.queue <- rep
}

func (r *Reporter) processReports(ctx context.Context, loggerHandle zerolog.Logger) {
	for rep := range r.queue {
		if rep.str != "" {
			r.file.WriteString(rep.str)
		} else {
			str := logger.ExtJSONString(rep.details)
			if r.jsonArray {
				str = "  " + str + ","
			}
			r.file.WriteString(str + "\n")
		}
	}
}
