package reporter

import (
	"context"
	"os"
	"skv/internal/logger"
	"skv/internal/worker"
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
	last    bool

	str string
}

func NewReporter(filePath string, jsonArary bool, rm bool) Reporter {
	if info, _ := os.Stat(filePath); info != nil {
		var err error
		if rm {
			err = os.Remove(filePath)
		} else {
			err = os.Rename(filePath, filePath+"."+time.Now().Local().Format(time.RFC3339)+".bak")
		}
		if err != nil {
			log.Fatal().Err(err)
		}
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

func (r *Reporter) Done(ctx context.Context) {
	log.Debug().Msg("closing reporter queue and waiting for reporters to finish")
	close(r.queue)
	r.pool.Done()
	r.file.Close()
}

func (r *Reporter) ReportString(value string) {
	rep := report{str: value}
	r.queue <- rep
}

func (r *Reporter) ReportValue(value bson.D, last bool) {
	rep := report{details: value, last: last}
	r.queue <- rep
}

func (r *Reporter) processReports(ctx context.Context, loggerHandle zerolog.Logger) {
	for rep := range r.queue {
		if rep.str != "" {
			r.file.WriteString(rep.str + "\n")
		} else {
			str := logger.ExtJSONString(rep.details)
			if r.jsonArray {
				str = "  " + str
				if !rep.last {
					str = str + ","
				}
			}
			r.file.WriteString(str + "\n")
		}
	}
}
