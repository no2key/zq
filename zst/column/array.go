package column

import (
	"errors"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type ArrayWriter struct {
	typ     zng.Type
	values  Writer
	lengths *IntWriter
}

func NewArrayWriter(inner zng.Type, spiller *Spiller) *ArrayWriter {
	return &ArrayWriter{
		typ:     inner,
		values:  NewWriter(inner, spiller),
		lengths: NewIntWriter(spiller),
	}
}

func (a *ArrayWriter) Write(body zcode.Bytes) error {
	it := zcode.Iter(body)
	var len int32
	for !it.Done() {
		body, _, err := it.Next()
		if err != nil {
			//XXX better error
			return err
		}
		if err := a.values.Write(body); err != nil {
			return err
		}
		len++
	}
	return a.lengths.Write(len)
}

func (a *ArrayWriter) Flush() error {
	if err := a.lengths.Flush(); err != nil {
		return err
	}
	return a.values.Flush()
}

func (a *ArrayWriter) Encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	b.BeginContainer()
	valType, err := a.values.Encode(zctx, b)
	if err != nil {
		return nil, err
	}
	lenType, err := a.lengths.Encode(zctx, b)
	if err != nil {
		return nil, err
	}
	b.EndContainer()
	cols := []zng.Column{
		{"values", valType},
		{"lengths", lenType},
	}
	return zctx.LookupTypeRecord(cols)
}

type ArrayReader struct {
	values  Reader
	lengths *IntReader
}

func NewArrayReader(inner zng.Type, reassembly zng.Value, r io.ReaderAt) (*ArrayReader, error) {
	typ, ok := reassembly.Type.(*zng.TypeRecord)
	if !ok {
		return nil, errors.New("zst object array_column not a record")
	}
	rec := zng.NewRecord(typ, reassembly.Bytes)
	zv, err := rec.Access("values")
	if err != nil {
		return nil, err
	}
	values, err := NewReader(inner, zv, r)
	if err != nil {
		return nil, err
	}
	zv, err = rec.Access("length")
	if err != nil {
		return nil, err
	}
	lengths, err := NewIntReader(zv, r)
	if err != nil {
		return nil, err
	}
	return &ArrayReader{
		values:  values,
		lengths: lengths,
	}, nil
}

func (a *ArrayReader) Read(b *zcode.Builder) error {
	len, err := a.lengths.Read()
	if err != nil {
		return err
	}
	b.BeginContainer()
	for k := 0; k < int(len); k++ {
		if err := a.values.Read(b); err != nil {
			return err
		}
	}
	b.EndContainer()
	return nil
}
