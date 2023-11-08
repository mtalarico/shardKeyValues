package sk

import (
	"context"
	"fmt"
	"sk/internal/cfg"
	"sk/internal/logger"
	"sk/internal/ns"
	"sk/internal/util"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ShardKeyDump struct {
	config          cfg.Configuration
	client          *mongo.Client
	hashedKey       string
	chunkFilterBase bson.D
}

func NewShardKeyDump(config cfg.Configuration, client *mongo.Client) ShardKeyDump {
	return ShardKeyDump{
		config: config,
		client: client,
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

func (s *ShardKeyDump) getNSFilter(collMeta ns.CollectionMetadata) bson.D {
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

func (s *ShardKeyDump) getKeyProjection(key bson.Raw) bson.D {
	var projection bson.D
	hasId := false
	keys, err := key.Elements()
	if err != nil {
		log.Fatal().Err(err)
	}
	for _, key := range keys {
		if key.Key() == "_id" {
			hasId = true
		}
		projection = append(projection, bson.E{key.Key(), 1})
	}
	if !hasId {
		projection = append(projection, bson.E{"_id", 0})
	}
	return projection
}

func (s *ShardKeyDump) getDistinctCursor(collMeta ns.CollectionMetadata) *mongo.Cursor {
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
	projection := s.getKeyProjection(collMeta.Key)
	min := util.MakeInfinity(collMeta.Key, util.MIN_KEY)
	max := util.MakeInfinity(collMeta.Key, util.MAX_KEY)
	log.Debug().Msg("made projection document: " + logger.ExtJSONString(projection))
	opts := options.Find().SetProjection(projection).SetHint(collMeta.Key)
	// if s.hashedKey != "" {
	// 	min = util.ReplaceValue(min, s.hashedKey, math.MinInt64)
	// 	max = util.ReplaceValue(max, s.hashedKey, math.MaxInt64)
	// }
	opts.SetMin(min).SetMax(max)

	cursor, err := collection.Find(context.TODO(), bson.D{}, opts)
	if err != nil {
		log.Fatal().Err(err)
	}
	return cursor
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
		log.Error().Msg("only expected one document to convert hash shard key, recieved: " + logger.ExtJSONString(cursorResults))
	}
	doc := cursorResults[0]

	return doc
}

func (s *ShardKeyDump) getRangeMetadata(key bson.Raw, min bson.Raw, max bson.Raw) bson.D {
	var minHash, maxHash bson.Raw
	minFilter, maxFilter := min, max
	datasizeFilter := bson.D{
		{"datasize", s.config.NS.String()},
		{"keyPattern", key},
	}
	if s.hashedKey != "" {
		minHash = s.ConvertToHashedShardKey(s.hashedKey, min)
		log.Trace().Msg("min hashed value doc: " + logger.ExtJSONString(minHash))
		minFilter = util.ReplaceValue(min, s.hashedKey, maxHash.Lookup("hashedValue"))

		if max.Lookup(s.hashedKey).Type != bson.TypeMaxKey {
			maxHash = s.ConvertToHashedShardKey(s.hashedKey, max)
			log.Trace().Msg("max hashed value doc: " + logger.ExtJSONString(maxHash))
			maxFilter = util.ReplaceValue(max, s.hashedKey, maxHash.Lookup("hashedValue"))
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
	log.Trace().Msg("recived datasize result: " + logger.ExtJSONString(bytes))
	meta := bson.D{
		{"key", min},
		{"size", bytes.Lookup("size")},
		{"count", bytes.Lookup("numObjects")},
	}
	if s.hashedKey != "" {
		meta = append(meta, bson.E{"hashedKey", minHash.Lookup("hashedValue")})
	}
	// is there a better way to do this? TODO... maybe cache chunks on the client side to avoid additional queries
	// if s.config.ChunkLookup {

	//   filter["min"] = { $lte: value.min };
	//   filter["max"] = { $gt: value.min };
	// chunk := s.client.Database("config").Collection("chunks").FindOne(context.TODO(), filter)
	//   range["shard"] = chunk.shard;
	//   range["chunk"] = {
	//     min: chunk.min,
	//     max: chunk.max,
	//   };
	//   // range["onCurrentShardSince"] = chunk.onCurrentShardSince;
	// }
	// }
	return meta
}

// get metadata for each unique shard key value
func (s *ShardKeyDump) ShardKeyValues() {
	util.EnsureMongos(s.client)
	meta := s.getCollMetadata()
	s.chunkFilterBase = s.getNSFilter(meta)
	s.hashedKey = util.HashedKey(meta.Key)
	cursor := s.getDistinctCursor(meta)
	defer cursor.Close(context.TODO())

	if !cursor.Next(context.TODO()) {
		log.Error().Msg("no docs found in collection")
		return
	}
	var min, max bson.Raw
	min = cursor.Current
	log.Trace().Msg("set min to " + logger.ExtJSONString(min))

	if s.config.JsonArray {
		fmt.Println("[")
	}
	for cursor.Next(context.TODO()) {
		max = cursor.Current
		log.Trace().Msg("set max to " + logger.ExtJSONString(max))

		minJson := logger.ExtJSONString(min)
		maxJson := logger.ExtJSONString(max)

		if minJson == maxJson {
			log.Trace().Msg("min " + minJson + " and max " + maxJson + " are equal, skipping")
			continue
		}
		valueMeta := s.getRangeMetadata(meta.Key, min, max)
		if s.config.JsonArray {
			fmt.Println("  " + logger.ExtJSONString(valueMeta) + ",")
		} else {
			fmt.Println(logger.ExtJSONString(valueMeta))
		}
		min = max
	}
	max = util.MakeInfinity(meta.Key, util.MAX_KEY)
	valueMeta := s.getRangeMetadata(meta.Key, min, max)
	if s.config.JsonArray {
		fmt.Println("  " + logger.ExtJSONString(valueMeta) + ",")
	} else {
		fmt.Println(logger.ExtJSONString(valueMeta))
	}
}
