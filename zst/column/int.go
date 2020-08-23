package column

import (
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
