package column

import (
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
}

func NewArrayReader(inner zng.Type, r io.Reader) (*ArrayReader, error) {
	//XXX
	return nil, nil
}

func (r *ArrayReader) Read() (zcode.Bytes, error) {
	//XXX
	return nil, nil
}
