package column

import (
	"errors"
	"io"

	"github.com/brimsec/zq/zng"
)

type PresenceWriter struct {
	IntWriter
	run   int32
	unset bool
}

func NewPresenceWriter(spiller *Spiller) *PresenceWriter {
	return &PresenceWriter{
		IntWriter: *NewIntWriter(spiller),
	}
}

func (p *PresenceWriter) TouchValue() {
	if !p.unset {
		p.run++
	} else {
		p.Write(p.run)
		p.run = 1
		p.unset = false
	}
}

func (p *PresenceWriter) TouchUnset() {
	if p.unset {
		p.run++
	} else {
		p.Write(p.run)
		p.run = 1
		p.unset = true
	}
}

func (p *PresenceWriter) Finish() {
	p.Write(p.run)
}

type PresenceReader struct {
	IntReader
	unset bool
	run   int
}

func NewPresenceReader(segmap zng.Value, r io.ReaderAt) (*PresenceReader, error) {
	ir, err := NewIntReader(segmap, r)
	if err != nil {
		return nil, err
	}
	run, err := ir.Read()
	if err != nil {
		return nil, err
	}
	return &PresenceReader{
		IntReader: *ir,
		run:       int(run),
	}, nil
}

func (p *PresenceReader) Read() (bool, error) {
	if p.run == 0 {
		p.unset = !p.unset
		run, err := p.IntReader.Read()
		if err != nil {
			return false, err
		}
		if run == 0 {
			// a zero run can only be the first value
			return false, errors.New("encountered illegal zero run in presence vector")
		}
		p.run = int(run)
	}
	p.run--
	return !p.unset, nil
}
