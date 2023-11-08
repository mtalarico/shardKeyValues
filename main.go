package main

import (
	"context"
	"skv/internal/cfg"
	"skv/internal/logger"
	"skv/internal/reporter"
	"skv/internal/sk"
	"skv/internal/util"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var skDump sk.ShardKeyDump

func connectMongo(config cfg.Configuration) *mongo.Client {
	options := options.Client().ApplyURI(config.URI)
	client, err := mongo.Connect(context.TODO(), options)
	if err != nil {
		panic(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
	log.Debug().Msg("Connected to " + util.Redact(config.URI))

	return client
}

func init() {
	config := cfg.Init()
	logger.Init(config.Verbosity, config.LogFile)
	config.Validate()

	log.Trace().Msgf("%#v", config)

	rep := reporter.NewReporter(config.ResultFile, config.JsonArray, config.RemoveResultsFile)
	client := connectMongo(config)
	skDump = sk.NewShardKeyDump(config, client, rep)
}

func main() {
	defer skDump.Disconnect()(context.TODO())
	skDump.ShardKeyValues()
}
