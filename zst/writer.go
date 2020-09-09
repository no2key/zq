package zst

import (
	"fmt"

	"github.com/brimsec/zq/pkg/bufwriter"
	"github.com/brimsec/zq/pkg/iosrc"
	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zio"
	"github.com/brimsec/zq/zio/zngio"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/brimsec/zq/zst/column"
)

//XXX TBD: implement skew threshold flushing
const (
	MaxSegmentThresh = column.MaxSegmentThresh
	MaxSkewThresh    = 512 * 1024 * 1024
)

// Writer implements the zbuf.Writer interface. A Writer creates a columnar
// zst object from a stream of zng.Records.
type Writer struct {
	uri        iosrc.URI
	zctx       *resolver.Context //XXX don't think we need this
	rctx       *resolver.Context
	writer     *bufwriter.Writer
	spiller    *column.Spiller
	schemaMap  map[int]int
	schemas    []column.RecordWriter
	types      []*zng.TypeRecord
	skewThresh int
	segThresh  int
	// We keep track of the size of rows we've encoded into in-memory
	// data structures.  This is roughtly propertional to the amount of
	// memory used and the max amount of skew between rows that will be
	// needed for reader-side buffering.  So when the memory footprint
	// exceeds the confired skew theshhold, we flush the columns to storage.
	footprint int
	root      *column.IntWriter
}

func NewWriter(zctx *resolver.Context, path string, skewThresh, segThresh int) (*Writer, error) {
	if err := checkThresh("skew", MaxSkewThresh, skewThresh); err != nil {
		return nil, err
	}
	if err := checkThresh("column", MaxSegmentThresh, segThresh); err != nil {
		return nil, err
	}
	uri, err := iosrc.ParseURI(path)
	if err != nil {
		return nil, err
	}
	w, err := iosrc.NewWriter(uri)
	if err != nil {
		return nil, err
	}
	writer := bufwriter.New(w)
	spiller := column.NewSpiller(writer, segThresh)
	return &Writer{
		zctx:       zctx,
		rctx:       resolver.NewContext(),
		uri:        uri,
		spiller:    spiller,
		writer:     writer,
		schemaMap:  make(map[int]int),
		skewThresh: skewThresh,
		segThresh:  segThresh,
		root:       column.NewIntWriter(spiller),
	}, nil
}

func checkThresh(which string, max, thresh int) error {
	if thresh == 0 {
		return fmt.Errorf("zst %s threshold cannot be zero", which)
	}
	if thresh > max {
		return fmt.Errorf("zst %s threshold too large (%d)", which, thresh)
	}
	return nil
}

func (w *Writer) Write(rec *zng.Record) error {
	inputID := rec.Type.ID()
	schemaID, ok := w.schemaMap[inputID]
	if !ok {
		recType, err := w.rctx.TranslateTypeRecord(rec.Type)
		if err != nil {
			return err
		}
		schemaID = len(w.schemas)
		w.schemaMap[inputID] = schemaID
		rw := column.NewRecordWriter(recType, w.spiller)
		w.schemas = append(w.schemas, rw)
		w.types = append(w.types, recType)
	}
	if err := w.root.Write(int32(schemaID)); err != nil {
		return err
	}
	if err := w.schemas[schemaID].Write(rec.Raw); err != nil {
		return err
	}
	w.footprint += len(rec.Raw)
	if w.footprint >= w.skewThresh {
		w.footprint = 0
		return w.flush()
	}
	return nil
}

// Abort closes this writer, deleting any and all objects and/or files associated
// with it.
func (w *Writer) Abort() error {
	firstErr := w.writer.Close()
	if err := iosrc.Remove(w.uri); firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (w *Writer) Close() error {
	if err := w.finalize(); err != nil {
		w.writer.Close()
		return err
	}
	return w.writer.Close()
}

func (w *Writer) flush() error {
	for _, col := range w.schemas {
		if err := col.Flush(); err != nil {
			return err
		}
	}
	return w.root.Flush()
}

func (w *Writer) finalize() error {
	if err := w.flush(); err != nil {
		return err
	}
	// at this point all the column data has been written out
	// to the underlying spiller.
	// we start writing zng at this point.
	zw := zngio.NewWriter(w.writer, zio.WriterFlags{})
	dataSize := w.spiller.Position()
	var b zcode.Builder
	// First, write out empty records for each schemas.  These types
	// are in the type context of the zng row reeader used to ingest
	// all of the data.  Since they are put here first, when they are
	// read fresh by the zst reader, the will exactly match the original schemas.
	for _, schema := range w.types {
		b.Reset()
		for _, col := range schema.Columns {
			if zng.IsContainerType(col.Type) {
				b.AppendContainer(nil)
			} else {
				b.AppendPrimitive(nil)
			}
		}
		rec := zng.NewRecord(schema, b.Bytes())
		if err := zw.Write(rec); err != nil {
			return err
		}
	}
	// Next, write the root reassembly record.
	b.Reset()
	typ, err := w.root.Encode(w.rctx, &b)
	if err != nil {
		return err
	}
	rootType, err := w.rctx.LookupTypeRecord([]zng.Column{{"root", typ}})
	if err != nil {
		return err
	}
	rec := zng.NewRecord(rootType, b.Bytes())
	if err := zw.Write(rec); err != nil {
		return err
	}
	// Now, write out the reassembly record for each schema.  Each record
	// is highly nested and encodes all of the segmaps for every column stream
	// needed to reconstruct all of the records of that schema.
	for _, schema := range w.schemas {
		b.Reset()
		typ, err := schema.Encode(w.rctx, &b)
		if err != nil {
			return err
		}
		body, err := b.Bytes().ContainerBody()
		if err != nil {
			return err
		}
		rec := zng.NewRecord(typ.(*zng.TypeRecord), body)
		if err := zw.Write(rec); err != nil {
			return err
		}
	}
	zw.EndStream()
	columnSize := zw.Position()
	sizes := []int64{dataSize, columnSize}
	return writeTrailer(zw, w.rctx, w.skewThresh, w.segThresh, sizes)
}

func (w *Writer) writeEmptyTrailer() error {
	zw := zngio.NewWriter(w.writer, zio.WriterFlags{})
	return writeTrailer(zw, w.rctx, w.skewThresh, w.segThresh, nil)
}

func writeTrailer(w *zngio.Writer, zctx *resolver.Context, skewThresh, segThresh int, sizes []int64) error {
	rec, err := newTrailerRecord(zctx, skewThresh, segThresh, sizes)
	if err != nil {
		return err
	}
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.EndStream()
}
