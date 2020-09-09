package zst

import (
	"errors"
	"fmt"
	"io"

	"github.com/brimsec/zq/pkg/iosrc"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zio/zngio"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/brimsec/zq/zst/column"
)

// Reader implements the zbuf.Reader and io.Closer.  It reads a columnar
// zst object to generate a stream of zng.Records.  It also has methods
// to read metainformation for test and debugging.
type Reader struct {
	reader     iosrc.Reader
	zctx       *resolver.Context
	uri        iosrc.URI
	root       *column.IntReader
	schemas    []column.RecordReader
	types      []*zng.TypeRecord
	trailer    *Trailer
	trailerLen int
	size       int64
	builder    zcode.Builder
}

// NewReader returns a Reader ready to read a microindex.
// Close() should be called when done.  This embeds a zngio.Seeker so
// Seek() may be called on this Reader.  Any call to Seek() must be to
// an offset that begins a new zng stream (e.g., beginning of file or
// the data immediately following an end-of-stream code)
func NewReader(zctx *resolver.Context, path string) (*Reader, error) {
	uri, err := iosrc.ParseURI(path)
	if err != nil {
		return nil, err
	}
	return NewReaderFromURI(zctx, uri)
}

func NewReaderFromURI(zctx *resolver.Context, uri iosrc.URI) (*Reader, error) {
	r, err := iosrc.NewReader(uri)
	if err != nil {
		return nil, err
	}
	// Grab the size so we don't seek past the front of the file and
	// cause an error.  XXX this causes an extra synchronous round-trip
	// in the inner loop of a microindex scan, so we might want to do this
	// in parallel with the open either by extending the iosrc interface
	// or running this call here in its own goroutine (before the open)
	si, err := iosrc.Stat(uri)
	if err != nil {
		return nil, err
	}
	size := si.Size()
	trailer, trailerLen, err := readTrailer(r, size)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("%s: %w", uri, err)
	}
	if trailer.SkewThresh > MaxSkewThresh {
		return nil, fmt.Errorf("%s: skew threshold too large (%d)", uri, trailer.SkewThresh)
	}
	if trailer.SegmentThresh > MaxSegmentThresh {
		return nil, fmt.Errorf("%s: column threshold too large (%d)", uri, trailer.SegmentThresh)
	}
	return &Reader{
		reader:     r,
		zctx:       zctx,
		uri:        uri,
		size:       size,
		trailer:    trailer,
		trailerLen: trailerLen,
	}, nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}

func (r *Reader) IsEmpty() bool {
	if r.trailer == nil {
		panic("IsEmpty called on a Reader with an error")
	}
	return r.trailer.Sections == nil
}

func (r *Reader) Read() (*zng.Record, error) {
	if r.schemas == nil {
		//XXX maybe do this in New?
		if err := r.init(); err != nil {
			return nil, err
		}
		if r.schemas == nil {
			return nil, errors.New("no schemas found")
		}
	}
	schemaID, err := r.root.Read()
	if err == io.EOF {
		return nil, nil
	}
	if schemaID < 0 || int(schemaID) >= len(r.schemas) {
		return nil, errors.New("bad schema id in root reassembly column")
	}
	r.builder.Reset()
	schema := r.schemas[schemaID]
	err = schema.Read(&r.builder)
	//XXX need to map r.types[] type to output zctx so the reassembly
	// types won't, in general, interfere with the output type context
	rec := zng.NewRecord(r.types[schemaID], r.builder.Bytes())
	//XXX if we had a buffer pool where records could be built back to
	// back in batches, then we could get rid of this extra allocation
	// and copy on every record
	rec.Keep()
	return rec, nil
}

func (r *Reader) init() error {
	reader := r.NewReassemblyReader()
	var rec *zng.Record
	for {
		rec, err := reader.Read()
		if err != nil {
			return err
		}
		if rec == nil {
			return errors.New("no reassembly records found in zst file")
		}
		zv := rec.Value(0)
		if zv.Bytes != nil {
			break
		}
		r.types = append(r.types, rec.Type)
	}
	segmap, err := rec.Access("root")
	if err != nil {
		return err
	}
	r.root, err = column.NewIntReader(segmap, r.reader)
	if err != nil {
		return err
	}
	for k := 0; k < len(r.types); k++ {
		rec, err := r.Read()
		if err != nil {
			return err
		}
		zv := zng.Value{rec.Type, rec.Raw}
		reader, err := column.NewRecordReader(r.types[k], zv, r.reader)
		if err != nil {
			return err
		}
		r.schemas = append(r.schemas, reader)
	}
	rec, _ = r.Read()
	if rec != nil {
		return errors.New("extra records in reassembly section")
	}
	return nil
}

//XXX this should be a common method on Trailer and shared with microindexaxs
func (r *Reader) section(level int) (int64, int64) {
	off := int64(0)
	for k := 0; k < level; k++ {
		off += r.trailer.Sections[k]
	}
	return off, r.trailer.Sections[level]
}

func (r *Reader) newSectionReader(zctx *resolver.Context, level int, sectionOff int64) zbuf.Reader {
	off, len := r.section(level)
	off += sectionOff
	len -= sectionOff
	reader := io.NewSectionReader(r.reader, off, len)
	return zngio.NewReader(reader, zctx)
}

func (r *Reader) NewReassemblyReader() zbuf.Reader {
	return r.newSectionReader(resolver.NewContext(), 1, 0)
}

func (r *Reader) NewTrailerReader() zbuf.Reader {
	off := r.size - int64(r.trailerLen)
	reader := io.NewSectionReader(r.reader, off, int64(r.trailerLen))
	return zngio.NewReaderWithOpts(reader, r.zctx, zngio.ReaderOpts{Size: r.trailerLen})
}
