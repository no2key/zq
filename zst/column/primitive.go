package column

import (
	"errors"
	"io"

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
	iter   zcode.Iter
	segmap []Segment
	reader io.ReaderAt
}

func NewPrimitiveReader(zv zng.Value, reader io.ReaderAt) (*PrimitiveReader, error) {
	segmap, err := parseSegmap(zv)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return &PrimitiveReader{
		segmap: segmap,
		reader: reader,
	}, nil
}

func (p *PrimitiveReader) Read(b *zcode.Builder) error {
	if p.iter == nil || p.iter.Done() {
		if len(p.segmap) == 0 {
			return io.EOF
		}
		if err := p.next(); err != nil {
			return err
		}
	}
	zv, _, err := p.iter.Next()
	if err != nil {
		return err
	}
	b.AppendPrimitive(zv)
	return nil
}

func (p *PrimitiveReader) next() error {
	segment := p.segmap[0]
	p.segmap = p.segmap[1:]
	if segment.Length > 2*MaxSegmentThresh {
		return errors.New("segment too big")
	}
	b := make([]byte, segment.Length)
	//XXX this where lots of seeks can happen until we put intelligent
	// scheduling in a layer below this informed by the reassembly maps
	// and the query that is going to run.
	n, err := p.reader.ReadAt(b, segment.Offset)
	if err != nil {
		return err
	}
	if n < int(segment.Length) {
		return errors.New("truncated read of zst column")
	}
	p.iter = zcode.Iter(b)
	return nil
}
