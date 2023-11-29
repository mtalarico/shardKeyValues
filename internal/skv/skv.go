package skv

import (
	"bytes"
	"context"
	"skv/internal/cfg"
	"skv/internal/logger"
	"skv/internal/ns"
	"skv/internal/reporter"
	"skv/internal/util"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type ShardKeyDump struct {
	config      cfg.Configuration
	client      *mongo.Client
	reporter    *reporter.Reporter
	chunkCursor *mongo.Cursor
	collCursor  *mongo.Cursor
}

func NewShardKeyDump(config cfg.Configuration, client *mongo.Client, reporter *reporter.Reporter) ShardKeyDump {
	return ShardKeyDump{
		config:   config,
		client:   client,
		reporter: reporter,
	}
}

func (s *ShardKeyDump) Disconnect() func(ctx context.Context) error {
	return s.client.Disconnect
}

func (s *ShardKeyDump) getCollMetadata() ns.CollectionMetadata {
	namespace := s.config.NS.String()
	result := s.client.Database("config").Collection("collections").FindOne(context.TODO(), bson.D{{"_id", namespace}})
	// not entirely sure why, but this was not decoding into collectionMeta directly, so this is manual. TODO to fix this
	raw, err := result.Raw()
	if err != nil {
		log.Fatal().Err(err)
	}
	subtype, data := raw.Lookup("uuid").Binary()
	meta := ns.CollectionMetadata{
		ID:   raw.Lookup("_id").StringValue(),
		UUID: primitive.Binary{Subtype: subtype, Data: data},
		Key:  raw.Lookup("key").Document(),
	}
	log.Debug().Msg("found collection metadata: " + meta.String())
	return meta
}

func (s *ShardKeyDump) ConvertToHashedShardKey(hashedKey string, key bson.Raw) bson.Raw {
	var cursorResults []bson.Raw
	unhashed := key.Lookup(hashedKey)
	docsStage := bson.D{{"$documents", bson.A{bson.D{{"value", unhashed}}}}}
	addFieldsStage := bson.D{{"$addFields", bson.D{{"hashedValue", bson.D{{"$toHashedIndexKey", "$value"}}}}}}
	cursor, err := s.client.Database("admin").Aggregate(context.TODO(), mongo.Pipeline{docsStage, addFieldsStage})
	if err != nil {
		log.Fatal().Err(err)
	}
	defer cursor.Close(context.TODO())
	if err = cursor.All(context.TODO(), &cursorResults); err != nil {
		log.Fatal().Err(err)
	}
	if len(cursorResults) != 1 {
		log.Error().Msg("only expected one document to convert hash shard key, recieved " + strconv.Itoa(len(cursorResults)))
	}
	doc := cursorResults[0]

	return doc
}

func (s *ShardKeyDump) getRangeMetadata(key bson.Raw, hashedKey string, min bson.Raw, max bson.Raw) bson.D {
	var minHash, maxHash bson.Raw
	minFilter, maxFilter := min, max
	datasizeFilter := bson.D{
		{"datasize", s.config.NS.String()},
		{"keyPattern", key},
	}
	if hashedKey != "" {
		minHash = s.ConvertToHashedShardKey(hashedKey, min)
		log.Trace().Msg("min hashed value doc: " + minHash.String())
		minFilter = util.ReplaceValue(min, hashedKey, minHash.Lookup("hashedValue"))

		// if we're on the final value of MaxKey, do not hash it, that could have disasterous results...
		if max.Lookup(hashedKey).Type != bson.TypeMaxKey {
			maxHash = s.ConvertToHashedShardKey(hashedKey, max)
			log.Trace().Msg("max hashed value doc: " + maxHash.String())
			maxFilter = util.ReplaceValue(max, hashedKey, maxHash.Lookup("hashedValue"))
		}
	}
	datasizeFilter = append(datasizeFilter, bson.E{"min", minFilter})
	datasizeFilter = append(datasizeFilter, bson.E{"max", maxFilter})
	log.Trace().Msg("sending command: " + logger.ExtJSONString(datasizeFilter))
	datasizeResult := s.client.Database(s.config.NS.Database).RunCommand(context.TODO(), datasizeFilter)
	bytes, err := datasizeResult.Raw()
	if err != nil {
		log.Fatal().Err(err)
	}
	log.Trace().Msg("recived datasize result: " + bytes.String())
	meta := bson.D{
		{"key", min},
	}
	if hashedKey != "" {
		meta = append(meta, bson.E{"hashedKey", minFilter})
	}
	meta = append(meta, bson.E{"size", bytes.Lookup("size")})
	meta = append(meta, bson.E{"count", bytes.Lookup("numObjects")})
	return meta
}

// get metadata for each unique shard key value
func (s *ShardKeyDump) ShardKeyValues() {
	util.EnsureMongos(s.client)
	if s.balancerEnabled() {
		defer s.setCollectionBalancerState(true)
	}
	s.setCollectionBalancerState(false)
	meta := s.getCollMetadata()
	log.Info().Msg("dumping shard key values for ns " + meta.ID + " and shard key " + meta.Key.String() + " to " + s.config.ResultFile)

	// init cursors
	s.initCoveredCursor(meta)
	s.initChunkCursor(meta)
	defer s.collCursor.Close(context.TODO())
	defer s.chunkCursor.Close(context.TODO())

	if !s.collCursor.Next(context.TODO()) {
		log.Error().Msg("no docs found in collection")
		return
	}
	if !s.chunkCursor.Next(context.TODO()) {
		log.Error().Msg("no docs found in chunks collection")
		return
	}
	var min, max bson.Raw
	min = s.collCursor.Current
	log.Trace().Msg("set min to " + min.String())

	// getting the hash key (if there is one) to avoid calculating each loop
	hashedKey := util.HashedKey(meta.Key)

	if s.config.JsonArray {
		s.reporter.ReportString("[")
	}
	for s.collCursor.Next(context.TODO()) {
		max = s.collCursor.Current
		log.Trace().Msg("set max to " + max.String())

		if bytes.Equal(min, max) {
			log.Trace().Msg("min " + min.String() + " and max " + max.String() + " are equal, skipping")
			continue
		}
		valueMeta := s.getRangeMetadata(meta.Key, hashedKey, min, max)
		chunkMeta := s.getChunkMetadata(min)
		valueMeta = append(valueMeta, chunkMeta...)
		s.reporter.ReportValue(valueMeta, false)
		min = max
	}
	valueMeta := s.getRangeMetadata(meta.Key, hashedKey, min, max)
	chunkMeta := s.getChunkMetadata(min)
	valueMeta = append(valueMeta, chunkMeta...)
	s.reporter.ReportValue(valueMeta, false)
	if s.config.JsonArray {
		s.reporter.ReportString("]")
	}
	s.reporter.Done(context.TODO())
}
