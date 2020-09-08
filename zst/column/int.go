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

func NewIntReader(reader io.Reader) (*IntReader, error) {
	p, err := NewPrimitiveReader(reader)
	if err != nil {
		return nil, err
	}
	return &IntReader{*p}, err
}

func (p *IntReader) Read() (int64, error) {
	b, err := p.PrimitiveReader.Read()
	if err != nil {
		return 0, err
	}
	return zng.DecodeInt(b)
}
