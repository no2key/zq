package column

import (
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

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
	Read() (zcode.Bytes, error)
}

func NewReader(typ zng.Type, r io.Reader) (Reader, error) {
	switch typ := typ.(type) {
	case *zng.TypeAlias:
		return NewReader(typ.Type, r)
	case *zng.TypeRecord:
		return NewRecordReader(typ, r)
	case *zng.TypeArray:
		return NewArrayReader(typ.Type, r)
	case *zng.TypeSet:
		// Sets encode the same as arrays but behave
		// different semantically and we don't care here.
		// XXX this isn't right.  TBD: fix.
		return NewArrayReader(typ.InnerType, r)
	case *zng.TypeUnion:
		return NewUnionReader(typ, r)
	default:
		return NewPrimitiveReader(r)
	}
}

type Segment struct {
	Offset int64
	Length int64
}

func (s Segment) NewSectionReader(r io.ReaderAt) io.Reader {
	return io.NewSectionReader(r, s.Offset, s.Length)
}
