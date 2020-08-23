package column

import (
	"errors"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type FieldWriter struct {
	name     string
	column   Writer
	presence *PresenceWriter
	vcnt     int
	ucnt     int
}

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
