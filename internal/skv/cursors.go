package skv

import (
	"context"
	"skv/internal/logger"
	"skv/internal/ns"
	"skv/internal/util"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (s *ShardKeyDump) initCoveredCursor(collMeta ns.CollectionMetadata) {
	collection := s.client.Database(s.config.NS.Database).Collection(s.config.NS.Collection)
	shardKey := collMeta.Key
	if !s.config.SkipIndexBuild {
		idx := mongo.IndexModel{Keys: shardKey}
		name, err := collection.Indexes().CreateOne(context.TODO(), idx)
		if err != nil {
			log.Fatal().Err(err)
		}
		log.Debug().Msg("ran createIndex for '" + name + "'")
	}
	projection := util.GetKeyProjection(collMeta.Key, false)
	sort := util.GetKeyProjection(collMeta.Key, true)
	min := util.MakeInfinity(collMeta.Key, util.MIN_KEY)
	log.Debug().Msg("made projection document: " + logger.ExtJSONString(projection))
	opts := options.Find().SetProjection(projection).SetHint(collMeta.Key)
	opts.SetMin(min).SetSort(sort)

	cursor, err := collection.Find(context.TODO(), bson.D{}, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot open cursor against collection")
	}
	s.collCursor = cursor
}

func (s *ShardKeyDump) initChunkCursor(collMeta ns.CollectionMetadata) {
	collection := s.client.Database("config").Collection("chunks")
	nsFilter := s.getChunkNSFilter(collMeta)

	log.Debug().Msg("getting chunk cursor with filter: " + logger.ExtJSONString(nsFilter))
	opts := options.Find().SetSort(bson.D{{"min", 1}}).SetNoCursorTimeout(true)
	cursor, err := collection.Find(context.TODO(), nsFilter, opts)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot open cursor against chunks")
	}
	// prime cursor and ensure it isn't empty
	if !cursor.Next(context.TODO()) {
		log.Fatal().Msg("no chunks found for collection")
	}
	s.chunkCursor = cursor
}

func (s *ShardKeyDump) getChunkNSFilter(collMeta ns.CollectionMetadata) bson.D {
	result := s.client.Database("admin").RunCommand(context.TODO(), bson.D{{"buildInfo", 1}})
	buildInfo, err := result.Raw()
	if err != nil {
		log.Fatal().Err(err)
	}
	versionArr, err := buildInfo.Lookup("versionArray").Array().Elements()
	major := versionArr[0].Value().AsInt64()
	if err != nil {
		log.Fatal().Err(err)
	}
	log.Debug().Msg("detected major version: " + strconv.FormatInt(major, 10))
	if major > 5 {
		return bson.D{{"uuid", collMeta.UUID}}
	} else {
		return bson.D{{"ns", collMeta.ID}}
	}
}

func (s *ShardKeyDump) getChunkMetadata(value bson.Raw) bson.D {
	gte := util.DocGteRangeBound(value, s.chunkCursor.Current)
	log.Trace().Bool("gteShardKey", gte).Msg("")

	if gte {
		s.chunkCursor.Next(context.TODO())
	}

	chunk := s.chunkCursor.Current
	// chunkFilter = append(chunkFilter, bson.E{"min", bson.D{{"$lte", value}}})
	// chunkFilter = append(chunkFilter, bson.E{"max", bson.D{{"$gt", value}}})
	return bson.D{
		{"shard", chunk.Lookup("shard")},
		{"chunk", bson.D{
			{"min", chunk.Lookup("min")},
			{"max", chunk.Lookup("max")},
		}},
	}
}
