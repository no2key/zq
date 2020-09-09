package column

import (
	"errors"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

const MaxSegmentThresh = 20 * 1024 * 1024

type Writer interface {
	// Write encodes the given value into memory.  When the column exceeds
	// a threshold, it is automatically flushed.  Flush may also be called
	// explicitly to push columns to storage and thus avoid too much row skew
	// between columns.
	Write(zcode.Bytes) error
	// Push all in-memory column data to storage.
	Flush() error
	// Encode is called after all data is flushed to build reassembly
	// information for this column.
	Encode(*resolver.Context, *zcode.Builder) (zng.Type, error)
}

func NewWriter(typ zng.Type, spiller *Spiller) Writer {
	switch typ := typ.(type) {
	case *zng.TypeAlias:
		return NewWriter(typ.Type, spiller)
	case *zng.TypeRecord:
		return NewRecordWriter(typ, spiller)
	case *zng.TypeArray:
		return NewArrayWriter(typ.Type, spiller)
	case *zng.TypeSet:
		// Sets encode the same as arrays but behave
		// different semantically and we don't care here.
		// XXX this isn't right.  TBD: fix.
		return NewArrayWriter(typ.InnerType, spiller)
	case *zng.TypeUnion:
		return NewUnionWriter(typ, spiller)
	default:
		return NewPrimitiveWriter(spiller)
	}
}

type Reader interface {
	Read(*zcode.Builder) error
}

func NewReader(typ zng.Type, reassembly zng.Value, r io.ReaderAt) (Reader, error) {
	switch typ := typ.(type) {
	case *zng.TypeAlias:
		return NewReader(typ.Type, reassembly, r)
	case *zng.TypeRecord:
		return NewRecordReader(typ, reassembly, r)
	case *zng.TypeArray:
		return NewArrayReader(typ.Type, reassembly, r)
	case *zng.TypeSet:
		// Sets encode the same as arrays but behave
		// different semantically and we don't care here.
		// XXX this isn't right.  TBD: fix.
		return NewArrayReader(typ.InnerType, reassembly, r)
	case *zng.TypeUnion:
		return NewUnionReader(typ, reassembly, r)
	default:
		return NewPrimitiveReader(reassembly, r)
	}
}

type Segment struct {
	Offset int64
	Length int64
}

func (s Segment) NewSectionReader(r io.ReaderAt) io.Reader {
	return io.NewSectionReader(r, s.Offset, s.Length)
}

var ErrCorruptSegment = errors.New("segmap value corrupt")

//XXX we need a zng unmarshaler
func parseSegment(zv zcode.Bytes) (Segment, error) {
	var s Segment
	it := zcode.Iter(zv)
	zv, isContainer, err := it.Next()
	if err != nil {
		return s, err
	}
	if isContainer {
		return s, ErrCorruptSegment
	}
	v, err := zng.DecodeInt(zv)
	if err != nil {
		return s, err
	}
	s.Offset = v
	zv, isContainer, err = it.Next()
	if err != nil {
		return s, err
	}
	if isContainer {
		return s, ErrCorruptSegment
	}
	v, err = zng.DecodeInt(zv)
	if err != nil {
		return s, err
	}
	s.Length = v
	return s, nil
}

func checkSegType(col zng.Column, which string) bool {
	return col.Name == which && col.Type == zng.TypeInt64
}

func parseSegmap(zv zng.Value) ([]Segment, error) {
	typ, ok := zv.Type.(*zng.TypeArray)
	if !ok {
		return nil, errors.New("zst object segmap not an array")
	}
	segType, ok := typ.Type.(*zng.TypeRecord)
	if !ok {
		return nil, errors.New("zst object segmap element not a record")
	}
	if len(segType.Columns) != 2 || !checkSegType(segType.Columns[0], "offset") || !checkSegType(segType.Columns[1], "length") {
		return nil, errors.New("zst object segmap element not a record[offset:int64,length:int64]")
	}
	var segmap []Segment
	it := zcode.Iter(zv.Bytes)
	for !it.Done() {
		zv, isContainer, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !isContainer {
			return nil, ErrCorruptSegment
		}
		segment, err := parseSegment(zv)
		if err != nil {
			return nil, err
		}
		segmap = append(segmap, segment)
	}
	return segmap, nil
}
