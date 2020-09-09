package column

import (
	"errors"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type RecordWriter []FieldWriter

func NewRecordWriter(typ *zng.TypeRecord, spiller *Spiller) RecordWriter {
	var r RecordWriter
	for _, col := range typ.Columns {
		fw := FieldWriter{
			name:     col.Name,
			column:   NewWriter(col.Type, spiller),
			presence: NewPresenceWriter(spiller),
		}
		r = append(r, fw)
	}
	return r
}

func (r RecordWriter) Write(body zcode.Bytes) error {
	it := body.Iter()
	for _, f := range r {
		if it.Done() {
			return errors.New("zng record value doesn't match column writer") //XXX
		}
		body, _, err := it.Next()
		if err != nil {
			return err
		}
		if body == nil {
			f.ucnt++
			f.presence.TouchUnset()
			continue
		}
		f.vcnt++
		f.presence.TouchValue()
		if err := f.column.Write(body); err != nil {
			return err
		}
	}
	if !it.Done() {
		return errors.New("zng record value doesn't match column writer") //XXX
	}
	return nil
}

func (r RecordWriter) Flush() error {
	// XXX we might want to arrange these flushes differently for locality
	for _, f := range r {
		if err := f.column.Flush(); err != nil {
			return err
		}
		if f.vcnt != 0 && f.ucnt != 0 {
			// if it's not all values or all unsets, then
			// flush and write out the presence vector.
			// Otherwise, there will be no values and in the presence
			// column and an empty segmap will be encoded for it.
			f.presence.Finish()
			if err := f.presence.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r RecordWriter) Encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	var columns []zng.Column
	b.BeginContainer()
	for _, f := range r {
		fieldType, err := f.encode(zctx, b)
		if err != nil {
			return nil, err
		}
		columns = append(columns, zng.Column{f.name, fieldType})
	}
	b.EndContainer()
	return zctx.LookupTypeRecord(columns)
}

type FieldWriter struct {
	name     string
	column   Writer
	presence *PresenceWriter
	vcnt     int
	ucnt     int
}

func (f *FieldWriter) encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	b.BeginContainer()
	colType, err := f.column.Encode(zctx, b)
	if err != nil {
		return nil, err
	}
	presenceType, err := f.presence.Encode(zctx, b)
	if err != nil {
		return nil, err
	}
	b.EndContainer()
	cols := []zng.Column{
		{"column", colType},
		{"presence", presenceType},
	}
	return zctx.LookupTypeRecord(cols)
}

type RecordReader []FieldReader

func NewRecordReader(typ *zng.TypeRecord, reassembly zng.Value, reader io.ReaderAt) (RecordReader, error) {
	var r RecordReader
	rtype, ok := reassembly.Type.(*zng.TypeRecord)
	if !ok {
		return nil, errors.New("corrupt zst object: record_column is not a record")
	}
	k := 0
	for it := zcode.Iter(reassembly.Bytes); !it.Done(); k++ {
		zv, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if k >= len(typ.Columns) {
			return nil, errors.New("mismatch between record type and record_column") //XXX
		}
		fieldType := typ.Columns[k].Type
		f, err := NewFieldReader(fieldType, zng.Value{rtype.Columns[k].Type, zv}, reader)
		if err != nil {
			return nil, err
		}
		r = append(r, *f)
	}
	return r, nil
}

func (r RecordReader) Read(b *zcode.Builder) error {
	b.BeginContainer()
	for _, f := range r {
		if err := f.Read(b); err != nil {
			return err
		}
	}
	b.EndContainer()
	return nil
}

type FieldReader struct {
	isContainer bool
	column      Reader
	presence    *PresenceReader
}

func NewFieldReader(typ zng.Type, reassembly zng.Value, r io.ReaderAt) (*FieldReader, error) {
	rtype, ok := reassembly.Type.(*zng.TypeRecord)
	if !ok {
		return nil, errors.New("zst object array_column not a record")
	}
	rec := zng.NewRecord(rtype, reassembly.Bytes)
	zv, err := rec.Access("column")
	if err != nil {
		return nil, err
	}
	column, err := NewReader(typ, zv, r)
	if err != nil {
		return nil, err
	}
	zv, err = rec.Access("presence")
	if err != nil {
		return nil, err
	}
	presence, err := NewPresenceReader(zv, r)
	return &FieldReader{
		isContainer: zng.IsContainerType(typ),
		column:      column,
		presence:    presence,
	}, nil
}

func (f *FieldReader) Read(b *zcode.Builder) error {
	isval, err := f.presence.Read()
	if err != nil {
		return err
	}
	if isval {
		return f.column.Read(b)
	}
	if f.isContainer {
		b.AppendContainer(nil)
	} else {
		b.AppendPrimitive(nil)
	}
	return nil
}
