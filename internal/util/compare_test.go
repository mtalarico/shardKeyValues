package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// -- BoundKey Tests --
func TestBoundKey_Min(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.MinKey{}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.MinKey{}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBoundKey_Max(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.MaxKey{}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.MaxKey{}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBoundKey_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.MinKey{}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.MaxKey{}}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestBoundKey_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.MaxKey{}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.MinKey{}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Null Tests --
func TestNull(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Null{}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Null{}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Int Tests --
func TestInt_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 0}})
	b, _ := bson.Marshal(bson.D{{"x", 1}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestInt_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 1}})
	b, _ := bson.Marshal(bson.D{{"x", 1}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestInt_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 2}})
	b, _ := bson.Marshal(bson.D{{"x", 1}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestInt64_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", int64(0)}})
	b, _ := bson.Marshal(bson.D{{"x", int64(1)}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestInt64_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", int64(1)}})
	b, _ := bson.Marshal(bson.D{{"x", int64(1)}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestInt64_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", int64(2)}})
	b, _ := bson.Marshal(bson.D{{"x", int64(1)}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Double Tests --
func TestDouble_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 0.0}})
	b, _ := bson.Marshal(bson.D{{"x", 1.0}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestDouble_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 1.0}})
	b, _ := bson.Marshal(bson.D{{"x", 1.0}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestDouble_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", 2.0}})
	b, _ := bson.Marshal(bson.D{{"x", 1.0}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Decimal128 Tests --
func TestDecimal128_AltB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("0")
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestDecimal128_AeqB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("1")
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestDecimal128_AgtB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("2")
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Symbol Tests --
func TestSymbol_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('a')}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('b')}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestSymbol_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('a')}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('a')}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestSymbol_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('b')}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Symbol('a')}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- String Tests --
func TestString_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", "testA"}})
	b, _ := bson.Marshal(bson.D{{"x", "testB"}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestString_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", "testA"}})
	b, _ := bson.Marshal(bson.D{{"x", "testA"}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestString_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", "testB"}})
	b, _ := bson.Marshal(bson.D{{"x", "testA"}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Binary Tests --
func TestBinary_AltB_DataDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestBinary_AeqB_DataDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBinary_AgtB_DataDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBinary_AltB_SubtypeDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 3, Data: []byte{8, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestBinary_AeqB_SubtypeDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 5, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{2, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBinary_AgtB_SubtypeDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 3, Data: []byte{1, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBinary_AltB_LenDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 3, Data: []byte{8, 1, 2, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 2, 3, 5}}}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestBinary_AeqB_LenDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{7, 1, 5, 3}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{2, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBinary_AgtB_LenDiff(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 4, Data: []byte{8, 1, 2, 3, 5}}}})
	b, _ := bson.Marshal(bson.D{{"x", primitive.Binary{Subtype: 3, Data: []byte{1, 1, 2, 3}}}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- ObjectID Tests --
// func TestObjectID_AltB(t *testing.T) {
// 	aVal, _ := primitive.ObjectIDFromHex("6565540a484b6a2f5a8d3ebd")
// 	bVal, _ := primitive.ObjectIDFromHex("65655448484b6a2f5a8d3ebe")
// 	a, _ := bson.Marshal(bson.D{{"x", aVal}})
// 	b, _ := bson.Marshal(bson.D{{"x", bVal}})
// 	res := DocGteRangeBound(a, b)
// 	expected := false
// 	assert.Equal(t, expected, res)
// }

func TestObjectID_AeqB(t *testing.T) {
	aVal, _ := primitive.ObjectIDFromHex("6565540a484b6a2f5a8d3ebd")
	bVal, _ := primitive.ObjectIDFromHex("6565540a484b6a2f5a8d3ebd")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// func TestObjectID_AgtB(t *testing.T) {
// 	aVal, _ := primitive.ObjectIDFromHex("65655448484b6a2f5a8d3ebe")
// 	bVal, _ := primitive.ObjectIDFromHex("6565540a484b6a2f5a8d3ebd")
// 	a, _ := bson.Marshal(bson.D{{"x", bVal}})
// 	b, _ := bson.Marshal(bson.D{{"x", aVal}})
// 	res := DocGteRangeBound(a, b)
// 	expected := true
// 	assert.Equal(t, expected, res)
// }

// -- Boolean Tests --
func TestBoolean_AltB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", false}})
	b, _ := bson.Marshal(bson.D{{"x", true}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestBoolean_AeqB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", true}})
	b, _ := bson.Marshal(bson.D{{"x", true}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestBoolean_AgtB(t *testing.T) {
	a, _ := bson.Marshal(bson.D{{"x", true}})
	b, _ := bson.Marshal(bson.D{{"x", false}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Date Tests  --
func TestDate_AltB(t *testing.T) {
	aVal, _ := time.Parse(time.RFC3339, "2022-01-01T00:00:00Z")
	bVal, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestDate_AeqB(t *testing.T) {
	aVal, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	bVal, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestDate_AgtB(t *testing.T) {
	aVal, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	bVal, _ := time.Parse(time.RFC3339, "2022-01-01T00:00:00Z")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Timestamp Tests  --
func TestTimestamp_AltB_Ord(t *testing.T) {
	aVal := primitive.Timestamp{T: 123, I: 0}
	bVal := primitive.Timestamp{T: 123, I: 1}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestTimestamp_AeqB_Ord(t *testing.T) {
	aVal := primitive.Timestamp{T: 123, I: 1}
	bVal := primitive.Timestamp{T: 123, I: 1}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestTimestamp_AgtB_Ord(t *testing.T) {
	aVal := primitive.Timestamp{T: 123, I: 1}
	bVal := primitive.Timestamp{T: 123, I: 0}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestTimestamp_AltB_Time(t *testing.T) {
	aVal := primitive.Timestamp{T: 122, I: 2}
	bVal := primitive.Timestamp{T: 123, I: 1}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestTimestamp_AeqB_Time(t *testing.T) {
	aVal := primitive.Timestamp{T: 123, I: 1}
	bVal := primitive.Timestamp{T: 123, I: 1}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestTimestamp_AgtB_Time(t *testing.T) {
	aVal := primitive.Timestamp{T: 123, I: 1}
	bVal := primitive.Timestamp{T: 122, I: 0}
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

// -- Mixed Numeric Tests --
func TestAInt32BDecimal128_AltB(t *testing.T) {
	aVal := 0
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestAInt32BDecimal128_AeqB(t *testing.T) {
	aVal := 1
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt32BDecimal128_AgtB(t *testing.T) {
	aVal := 2
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt32BDouble_AltB(t *testing.T) {
	aVal := 0
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestAInt32BDouble_AeqB(t *testing.T) {
	aVal := 1
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt32BDouble_AgtB(t *testing.T) {
	aVal := 2
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt64BDecimal128_AltB(t *testing.T) {
	aVal := int64(0)
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestAInt64BDecimal128_AeqB(t *testing.T) {
	aVal := int64(1)
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt64BDecimal128_AgtB(t *testing.T) {
	aVal := int64(2)
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt64BDouble_AltB(t *testing.T) {
	aVal := int64(0)
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestAInt64BDouble_AeqB(t *testing.T) {
	aVal := int64(1)
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestAInt64BDouble_AgtB(t *testing.T) {
	aVal := int64(2)
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADoubleBInt32_AltB(t *testing.T) {
	aVal := 0.0
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestADoubleBInt32_AeqB(t *testing.T) {
	aVal := 1.0
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADoubleBInt32_AgtB(t *testing.T) {
	aVal := 2.0
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADoubleBDecimal128_AltB(t *testing.T) {
	aVal := 0.0
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestADoubleBDecimal128_AeqB(t *testing.T) {
	aVal := 1.0
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADoubleBDecimal128_AgtB(t *testing.T) {
	aVal := 2.0
	bVal, _ := primitive.ParseDecimal128("1")
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADecimal128BInt32_AltB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("0")
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestADecimal128BInt32_AeqB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("1")
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADecimal128BInt32_AgtB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("2")
	bVal := 1
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADecimal128BDouble_AltB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("0")
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := false
	assert.Equal(t, expected, res)
}

func TestADecimal128BDouble_AeqB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("1")
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}

func TestADecimal128BDouble_AgtB(t *testing.T) {
	aVal, _ := primitive.ParseDecimal128("2")
	bVal := 1.0
	a, _ := bson.Marshal(bson.D{{"x", aVal}})
	b, _ := bson.Marshal(bson.D{{"x", bVal}})
	res := DocGteRangeBound(a, b)
	expected := true
	assert.Equal(t, expected, res)
}
