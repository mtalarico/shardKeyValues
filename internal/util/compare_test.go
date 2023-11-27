package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestIntEqualSimple(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 1}})
	b, _ := bson.Marshal(bson.D{{"x", 1}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestIntBGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 1}})
	b, _ := bson.Marshal(bson.D{{"x", 2}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestIntAGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 3}})
	b, _ := bson.Marshal(bson.D{{"x", 2}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestInt64AGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", int64(3)}})
	b, _ := bson.Marshal(bson.D{{"x", int64(2)}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestInt128AGte(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("3")
	bVal, _ := primitive.ParseDecimal128("2")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestDoubleAGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 3.0}})
	b, _ := bson.Marshal(bson.D{{"x", 2.0}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestDateAGt(t *testing.T) {
	aT, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	bT, _ := time.Parse(time.RFC3339, "2022-01-01T00:00:00Z")
	aVal := primitive.NewDateTimeFromTime(aT)
	bVal := primitive.NewDateTimeFromTime(bT)
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestStringAGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", "b"}})
	b, _ := bson.Marshal(bson.D{{"x", "b"}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestStringUnequalAGt(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", "b"}, {"y", 1}})
	b, _ := bson.Marshal(bson.D{{"x", "b"}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// func TestMixedTpes(t *testing.T) {
// 	a, _ := bson.Marshal(bson.D{{"y", "str"}, {"z", bson.D{{"a", 1}, {"b", true}}}})
// 	b, _ := bson.Marshal(bson.D{{"x", nil}, {"y", "str"}, {"z", bson.D{{"a", 1}, {"b", true}}}})

// 	res := DocGteRangeBound(a, b)
// 	expected := true
// 	assert.Equal(t, expected, res)
// }
