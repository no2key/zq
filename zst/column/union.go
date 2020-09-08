package column

import (
	"fmt"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type UnionWriter struct {
	typ      *zng.TypeUnion
	columns  []Writer
	selector *IntWriter
}

func NewUnionWriter(typ *zng.TypeUnion, spiller *Spiller) *UnionWriter {
	var columns []Writer
	for _, typ := range typ.Types {
		columns = append(columns, NewWriter(typ, spiller))
	}
	return &UnionWriter{
		columns:  columns,
		selector: NewIntWriter(spiller),
	}
}

func (u *UnionWriter) Write(body zcode.Bytes) error {
	_, selector, zv, err := u.typ.SplitZng(body)
	if err != nil {
		return err
	}
	if int(selector) >= len(u.columns) || selector < 0 {
		return fmt.Errorf("bad selector in column.UnionWriter: %d", selector)
	}
	if err := u.selector.Write(int32(selector)); err != nil {
		return err
	}
	return u.columns[selector].Write(zv)
}

func (u *UnionWriter) Flush() error {
	if err := u.selector.Flush(); err != nil {
		return err
	}
	for _, col := range u.columns {
		if err := col.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (u *UnionWriter) Encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	var cols []zng.Column
	b.BeginContainer()
	for k, col := range u.columns {
		typ, err := col.Encode(zctx, b)
		if err != nil {
			return nil, err
		}
		// Field name is based on integer position in the column.
		name := fmt.Sprintf("c%d", k)
		cols = append(cols, zng.Column{name, typ})
	}
	typ, err := u.selector.Encode(zctx, b)
	if err != nil {
		return nil, err
	}
	cols = append(cols, zng.Column{"selector", typ})
	b.EndContainer()
	return zctx.LookupTypeRecord(cols)
}

type UnionReader struct {
}

func NewUnionReader(typ zng.Type, r io.Reader) (*UnionReader, error) {
	//XXX
	return nil, nil
}

func (r *UnionReader) Read() (zcode.Bytes, error) {
	//XXX
	return nil, nil
}
