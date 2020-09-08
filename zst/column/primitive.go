package column

import (
	"io"
	"io/ioutil"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type PrimitiveWriter struct {
	bytes    zcode.Bytes
	spiller  *Spiller
	segments []Segment
}

func NewPrimitiveWriter(spiller *Spiller) *PrimitiveWriter {
	return &PrimitiveWriter{
		spiller: spiller,
	}
}

func (p *PrimitiveWriter) Write(body zcode.Bytes) error {
	p.bytes = zcode.AppendPrimitive(p.bytes, body)
	var err error
	if len(p.bytes) >= p.spiller.Thresh {
		err = p.Flush()
	}
	return err
}

func (p *PrimitiveWriter) Flush() error {
	var err error
	if len(p.bytes) > 0 {
		p.segments, err = p.spiller.Write(p.segments, p.bytes)
		p.bytes = p.bytes[:0]
	}
	return err
}

const SegmapTypeString = "array[record[offset:int64,length:int32]]"

func (p *PrimitiveWriter) Encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	b.BeginContainer()
	for _, segment := range p.segments {
		// add a segmap record to the array for each segment
		b.BeginContainer()
		b.AppendPrimitive(zng.EncodeInt(segment.Offset))
		b.AppendPrimitive(zng.EncodeInt(segment.Length))
		b.EndContainer()
	}
	b.EndContainer()
	return zctx.LookupByName(SegmapTypeString)
}

type PrimitiveReader struct {
	iter zcode.Iter
}

func NewPrimitiveReader(reader io.Reader) (*PrimitiveReader, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return &PrimitiveReader{
		iter: zcode.Iter(b),
	}, nil
}

func (p *PrimitiveReader) Read() (zcode.Bytes, error) {
	zv, _, err := p.iter.Next()
	return zv, err
}
