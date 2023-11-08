package ns

import (
	"sk/internal/logger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Namespace struct {
	Database   string
	Collection string
}

func (ns Namespace) String() string {
	return ns.Database + "." + ns.Collection
}

type CollectionMetadata struct {
	ID   string           `bson:"_id"`
	UUID primitive.Binary `bson:"uuid"`
	Key  bson.Raw         `bson:"key"`
}

func (md CollectionMetadata) String() string {
	return logger.ExtJSONString(md)
}
