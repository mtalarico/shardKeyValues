package util

import (
	"context"
	"regexp"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func Redact(uri string) string {
	re := regexp.MustCompile(`\:\/\/(.*?)\:(.*?)\@`)
	return re.ReplaceAllString(uri, "://xxxx:xxxx@")
}

func HashedKey(doc bson.Raw) string {
	elems, err := doc.Elements()
	if err != nil {
		log.Fatal().Err(err)
	}
	for _, elem := range elems {
		if elem.Value().Type == bson.TypeString && elem.Value().StringValue() == "hashed" {
			return elem.Key()
		}
	}
	return ""
}

func EnsureMongos(client *mongo.Client) {
	result := client.Database("admin").RunCommand(context.TODO(), bson.D{{"isdbgrid", 1}})
	res, err := result.Raw()
	if err != nil {
		code := res.Lookup("code").AsInt64()
		if code == 59 {
			log.Fatal().Msg("not mongos, please run against mongos processes only")
		} else {
			log.Fatal().Err(err)
		}
	}
}

func ReplaceValue(doc bson.Raw, key string, value interface{}) bson.Raw {
	newDoc := bson.D{}
	elems, err := doc.Elements()
	if err != nil {
		log.Fatal().Err(err)
	}
	for _, elem := range elems {
		if elem.Key() == key {
			newDoc = append(newDoc, bson.E{key, value})
		} else {
			newDoc = append(newDoc, bson.E{elem.Key(), elem.Value()})
		}
	}
	raw, err := bson.Marshal(newDoc)
	if err != nil {
		log.Fatal().Err(err)
	}
	return raw
}

func MakeInfinity(key bson.Raw, mode Bound) bson.Raw {
	var doc bson.D
	keys, err := key.Elements()
	if err != nil {
		log.Fatal().Err(err)
	}
	for _, each := range keys {
		if mode == MIN_KEY {
			doc = append(doc, bson.E{each.Key(), primitive.MinKey{}})
		} else {
			doc = append(doc, bson.E{each.Key(), primitive.MaxKey{}})
		}
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		log.Fatal().Err(err)
	}
	return raw
}

func Max(a int, b int) int {
	switch a > b {
	case true:
		return a
	default:
		return b
	}
}

func GetKeyProjection(key bson.Raw, skipId bool) bson.D {
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
	if !hasId && !skipId {
		projection = append(projection, bson.E{"_id", 0})
	}
	return projection
}
