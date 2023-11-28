// package util

// import (
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// 	"go.mongodb.org/mongo-driver/bson"
// )

// // -- BoundKey Tests --
// func TestMakeSort(t *testing.T) {
// 	key, _ := bson.Marshal(bson.D{{"x", 1}, {"_id", 1}})
// 	res := GetKeyProjection(key, true)
// 	expected := bson.D{{"x", 1}, {"_id", 1}}
// 	assert.Equal(t, expected, res)
// }

// func TestMakeSort_Hashed(t *testing.T) {
// 	key, _ := bson.Marshal(bson.D{{"x", "hashed"}})
// 	res := GetKeyProjection(key, true)
// 	expected := bson.D{{"x", 1}}
// 	assert.Equal(t, expected, res)
// }

// func TestMakeSort_Hashed_Compound(t *testing.T) {
// 	key, _ := bson.Marshal(bson.D{{"x", "hashed"}, {"y", 1}})
// 	res := GetKeyProjection(key, true)
// 	expected := bson.D{{"x", 1}, {"y", 1}}
// 	assert.Equal(t, expected, res)
// }
