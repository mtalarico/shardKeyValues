package util

import (
	"bytes"
	"math/big"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	case bson.TypeEmbeddedDocument: // unsupported
		return 5
	case bson.TypeArray: // shard key value cant be an array
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
	case bson.TypeRegex: // unsupported
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

		docType := docValue.Type
		rngType := rngBoundV.Type

		// passed the non-equality check, types are equal
		switch rngType {

		// types have a cardinality of one, if their types are equal they must be equal
		case bson.TypeMaxKey, bson.TypeMinKey, bson.TypeNull:
			continue

		case bson.TypeInt64, bson.TypeInt32:
			switch docType {

			// both int64
			case bson.TypeInt64, bson.TypeInt32:
				if docValue.AsInt64() == rngBoundV.AsInt64() {
					continue
				}
				return docValue.AsInt64() > rngBoundV.AsInt64()
			// rngType is int64, docType is dec128
			case bson.TypeDecimal128:
				ai, ae, err := docValue.Decimal128().BigInt()
				if err != nil {
					log.Fatal().Err(err)
				}

				strValue := strconv.FormatInt(rngBoundV.AsInt64(), 10)
				b, err := primitive.ParseDecimal128(strValue)
				if err != nil {
					log.Fatal().Err(err)
				}
				bi, be, err := b.BigInt()
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
				return ae > be
			// rngType is int64, docType is double
			case bson.TypeDouble:
				if docValue.Double() == float64(rngBoundV.AsInt64()) {
					continue
				}
				return docValue.Double() > float64(rngBoundV.AsInt64())
			}

		case bson.TypeDouble:
			switch docType {
			// rngType is double, docType is int64
			case bson.TypeInt64, bson.TypeInt32:
				if float64(docValue.AsInt64()) == rngBoundV.Double() {
					continue
				}
				return float64(docValue.AsInt64()) > rngBoundV.Double()
			// rngType is double, docType is dec128
			case bson.TypeDecimal128:
				ai, ae, err := docValue.Decimal128().BigInt()
				if err != nil {
					log.Fatal().Err(err)
				}

				fstr := strconv.FormatFloat(rngBoundV.Double(), 'e', -1, 64)
				b, err := primitive.ParseDecimal128(fstr)
				if err != nil {
					log.Fatal().Err(err)
				}
				bi, be, err := b.BigInt()
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
				return ae > be
			// both double
			case bson.TypeDouble:
				if docValue.Double() == rngBoundV.Double() {
					continue
				}
				return docValue.Double() > rngBoundV.Double()
			}

		case bson.TypeDecimal128:
			var ai *big.Int
			var ae int

			switch docType {
			// rngType is dec128, docType is int64
			case bson.TypeInt64, bson.TypeInt32:
				strValue := strconv.FormatInt(docValue.AsInt64(), 10)
				a, err := primitive.ParseDecimal128(strValue)
				if err != nil {
					log.Fatal().Err(err)
				}
				ai, ae, err = a.BigInt()
				if err != nil {
					log.Fatal().Err(err)
				}
			// both dec128
			case bson.TypeDecimal128:
				ai, ae, err = docValue.Decimal128().BigInt()
				if err != nil {
					log.Fatal().Err(err)
				}
			// rngType is dec128, docType is double
			case bson.TypeDouble:
				fstr := strconv.FormatFloat(docValue.Double(), 'e', -1, 64)
				a, err := primitive.ParseDecimal128(fstr)
				if err != nil {
					log.Fatal().Err(err)
				}
				ai, ae, err = a.BigInt()
				if err != nil {
					log.Fatal().Err(err)
				}
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
			return ae > be

		case bson.TypeSymbol:
			cmp := bytes.Compare([]byte(docValue.Symbol()), []byte(rngBoundV.Symbol()))
			if cmp == 0 {
				continue
			}
			return bytes.Compare([]byte(docValue.Symbol()), []byte(rngBoundV.Symbol())) > 0

		case bson.TypeString:
			cmp := bytes.Compare([]byte(docValue.StringValue()), []byte(rngBoundV.StringValue()))
			if cmp == 0 {
				continue
			}
			return bytes.Compare([]byte(docValue.StringValue()), []byte(rngBoundV.StringValue())) > 0

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

		case bson.TypeObjectID:
			if docValue.ObjectID() == rngBoundV.ObjectID() {
				continue
			}
			return docValue.ObjectID().Hex() > rngBoundV.ObjectID().Hex()

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

		case bson.TypeDateTime:
			if docValue.DateTime() == rngBoundV.DateTime() {
				continue
			}
			return docValue.DateTime() > rngBoundV.DateTime()

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

		default:
			log.Fatal().Msg("type not implemented yet")
		}
	}
	// if we got out of that loop without returning, they are equal
	return true
}
