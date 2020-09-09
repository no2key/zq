package column

import (
	"io"

	"github.com/brimsec/zq/zng"
)

// just a helpful wrapper
type IntWriter struct {
	PrimitiveWriter
}

func NewIntWriter(spiller *Spiller) *IntWriter {
	return &IntWriter{*NewPrimitiveWriter(spiller)}
}

func (p *IntWriter) Write(v int32) error {
	return p.PrimitiveWriter.Write(zng.EncodeInt(int64(v)))
}

type IntReader struct {
	PrimitiveReader
}

func NewIntReader(segmap zng.Value, reader io.ReaderAt) (*IntReader, error) {
	p, err := NewPrimitiveReader(segmap, reader)
	if err != nil {
		return nil, err
	}
	return &IntReader{*p}, err
}

func (p *IntReader) Read() (int32, error) {
	zv, _, err := p.iter.Next()
	if err != nil {
		return 0, err
	}
	v, err := zng.DecodeInt(zv)
	return int32(v), err
}
