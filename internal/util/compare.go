package util

import (
	"bytes"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func typeRank(t bsontype.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 1
	case bson.TypeNull:
		return 2
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return 3
	case bson.TypeSymbol, bson.TypeString:
		return 4
	case bson.TypeEmbeddedDocument:
		return 5
	case bson.TypeArray:
		return 6
	case bson.TypeBinary:
		return 7
	case bson.TypeObjectID:
		return 8
	case bson.TypeBoolean:
		return 9
	case bson.TypeDateTime:
		return 10
	case bson.TypeTimestamp:
		return 11
	case bson.TypeRegex:
		return 12
	case bson.TypeMaxKey:
		return 13
	default:
		return 0
	}
}

func DocGteRangeBound(doc, rngBound bson.Raw) bool {
	rngBoundPairs, err := rngBound.Elements()
	if err != nil {
		log.Fatal().Err(err)
	}
	for _, rngBoundKV := range rngBoundPairs {
		currentKey := rngBoundKV.Key()
		rngBoundV := rngBoundKV.Value()

		docValue, err := doc.LookupErr(currentKey)
		// if the value is not included, it is treated as null since we have a unique index on the shard key
		if err == bsoncore.ErrElementNotFound {
			docValue = bson.RawValue{Type: bson.TypeNull, Value: nil}
		} else if err != nil {
			log.Error().Msg("lookup error")
		}

		rngBoundRank := typeRank(rngBoundV.Type)
		log.Trace().Int("Range Bound Rank: ", rngBoundRank).Str("key", currentKey).Msg("")

		docRank := typeRank(docValue.Type)
		log.Trace().Int("Doc Rank: ", docRank).Str("key", currentKey).Msg("")

		if docRank != rngBoundRank {
			return docRank > rngBoundRank
		}

		switch rngBoundV.Type {
		case bson.TypeInt64, bson.TypeInt32:
			if docValue.AsInt64() == rngBoundV.AsInt64() {
				continue
			}
			return docValue.AsInt64() > rngBoundV.AsInt64()
		case bson.TypeDateTime:
			if docValue.DateTime() == rngBoundV.DateTime() {
				continue
			}
			return docValue.DateTime() > rngBoundV.DateTime()
		case bson.TypeDecimal128:
			ai, ae, err := docValue.Decimal128().BigInt()
			if err != nil {
				log.Fatal().Err(err)
			}
			bi, be, err := rngBoundV.Decimal128().BigInt()
			if err != nil {
				log.Fatal().Err(err)
			}
			if ae == be {
				cmp := ai.Cmp(bi)
				if cmp == 0 {
					continue
				}
				return cmp > 0
			}
			return ae >= be

		case bson.TypeDouble:
			if docValue.Double() == rngBoundV.Double() {
				continue
			}
			return docValue.Double() > rngBoundV.Double()
		//  a   b     a > b
		//  f   f     no
		//  f   t     no
		//  t   t     no
		//  t   f     yes
		case bson.TypeBoolean:
			if docValue.Boolean() == rngBoundV.Boolean() {
				continue
			}
			return docValue.Boolean() && !rngBoundV.Boolean()
		case bson.TypeString:
			cmp := bytes.Compare([]byte(docValue.StringValue()), []byte(rngBoundV.StringValue()))
			if cmp == 0 {
				continue
			}
			return bytes.Compare([]byte(docValue.StringValue()), []byte(rngBoundV.StringValue())) > 0
		case bson.TypeTimestamp:
			at, ao := docValue.Timestamp()
			bt, bo := rngBoundV.Timestamp()
			if at == bt {
				if ao == bo {
					continue
				}
				return ao > bo
			}
			return at > bt
		case bson.TypeObjectID:
			if docValue.ObjectID().String() == rngBoundV.ObjectID().String() {
				continue
			}
			return docValue.ObjectID().String() > rngBoundV.ObjectID().String()
		// MongoDB sorts BinData in the following order:
		//   First, the length or size of the data.
		//   Then, by the BSON one-byte subtype.
		//   Finally, by the data, performing a byte-by-byte comparison.
		case bson.TypeBinary:
			abst, abd := docValue.Binary()
			bbst, bbd := rngBoundV.Binary()
			abl := len(abd)
			bbl := len(bbd)
			if abl == bbl {
				if abst == bbst {
					cmp := bytes.Compare(abd, bbd)
					if cmp == 0 {
						continue
					}
					return cmp > 0
				}
				return abst > bbst
			}
			return abl > bbl
		// types have a cardinality of one, if their types are equal they must be equal
		case bson.TypeMaxKey, bson.TypeMinKey:
			continue
		default:
			log.Fatal().Msg("type not implemented yet")
		}
	}
	// if we got out of that loop without returning, they are equal
	return true
}
