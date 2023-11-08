package main

import (
	"context"
	"sk/internal/cfg"
	"sk/internal/logger"
	"sk/internal/sk"
	"sk/internal/util"
	"time"

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
	startTime := time.Now()
	config := cfg.Init()
	logger.Init(config.Verbosity, config.LogFile, startTime)
	config.Validate()

	log.Trace().Msgf("%#v", config)

	client := connectMongo(config)
	skDump = sk.NewShardKeyDump(config, client)
}

func main() {
	defer skDump.Disconnect()(context.TODO())
	skDump.ShardKeyValues()
}
