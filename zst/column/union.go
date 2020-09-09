package column

import (
	"errors"
	"fmt"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

type UnionWriter struct {
	typ      *zng.TypeUnion
	values   []Writer
	selector *IntWriter
}

func NewUnionWriter(typ *zng.TypeUnion, spiller *Spiller) *UnionWriter {
	var values []Writer
	for _, typ := range typ.Types {
		values = append(values, NewWriter(typ, spiller))
	}
	return &UnionWriter{
		values:   values,
		selector: NewIntWriter(spiller),
	}
}

func (u *UnionWriter) Write(body zcode.Bytes) error {
	_, selector, zv, err := u.typ.SplitZng(body)
	if err != nil {
		return err
	}
	if int(selector) >= len(u.values) || selector < 0 {
		return fmt.Errorf("bad selector in column.UnionWriter: %d", selector)
	}
	if err := u.selector.Write(int32(selector)); err != nil {
		return err
	}
	return u.values[selector].Write(zv)
}

func (u *UnionWriter) Flush() error {
	if err := u.selector.Flush(); err != nil {
		return err
	}
	for _, value := range u.values {
		if err := value.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (u *UnionWriter) Encode(zctx *resolver.Context, b *zcode.Builder) (zng.Type, error) {
	var cols []zng.Column
	b.BeginContainer()
	for k, value := range u.values {
		typ, err := value.Encode(zctx, b)
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
	values   []Reader
	selector *IntReader
}

func NewUnionReader(typ *zng.TypeUnion, reassembly zng.Value, r io.ReaderAt) (*UnionReader, error) {
	rtype, ok := reassembly.Type.(*zng.TypeRecord)
	if !ok {
		return nil, errors.New("zst object union_column not a record")
	}
	rec := zng.NewRecord(rtype, reassembly.Bytes)
	var values []Reader
	k := 0
	for {
		zv, err := rec.Access(fmt.Sprintf("c%d", k))
		if err != nil {
			return nil, err
		}
		if k >= len(typ.Types) {
			return nil, errors.New("zst object too many union columns for union type")
		}
		valueCol, err := NewReader(typ.Types[k], zv, r)
		if err != nil {
			return nil, err
		}
		values = append(values, valueCol)
		k++
	}
	zv, err := rec.Access("selector")
	if err != nil {
		return nil, err
	}
	selector, err := NewIntReader(zv, r)
	if err != nil {
		return nil, err
	}
	return &UnionReader{
		values:   values,
		selector: selector,
	}, nil
}

func (u *UnionReader) Read(b *zcode.Builder) error {
	selector, err := u.selector.Read()
	if err != nil {
		return err
	}
	if selector < 0 || int(selector) >= len(u.values) {
		return errors.New("bad selector in zst union reader") //XXX
	}
	b.BeginContainer()
	b.AppendPrimitive(zng.EncodeInt(int64(selector)))
	if err := u.values[selector].Read(b); err != nil {
		return err
	}
	b.EndContainer()
	return nil
}
