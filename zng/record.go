package zng

import (
	"fmt"
	"strings"

	"github.com/brimsec/zq/zcode"
)

type TypeRecord struct {
	id      int
	Columns []Column
	LUT     map[string]int
}

func NewTypeRecord(id int, columns []Column) *TypeRecord {
	r := &TypeRecord{
		id:      id,
		Columns: columns,
	}
	r.createLUT()
	return r
}

func TypeRecordString(columns []Column) string {
	return ColumnString("record[", columns, "]")
}

func (t *TypeRecord) ID() int {
	return t.id
}

func (t *TypeRecord) SetID(id int) {
	t.id = id
}

func (t *TypeRecord) String() string {
	return TypeRecordString(t.Columns)
}

//XXX we shouldn't need this... tests are using it
func (t *TypeRecord) Decode(zv zcode.Bytes) ([]Value, error) {
	if zv == nil {
		return nil, ErrUnset
	}
	var vals []Value
	for i, it := 0, zcode.Iter(zv); !it.Done(); i++ {
		val, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if i >= len(t.Columns) {
			return nil, fmt.Errorf("too many values for record element %s", val)
		}
		v := Value{t.Columns[i].Type, val}
		vals = append(vals, v)
	}
	return vals, nil
}

func (t *TypeRecord) Parse(in []byte) (zcode.Bytes, error) {
	panic("record.Parse shouldn't be called")
}

func (t *TypeRecord) StringOf(zv zcode.Bytes, fmt OutFmt, _ bool) string {
	var b strings.Builder
	separator := byte(',')
	switch fmt {
	case OutFormatZNG:
		b.WriteByte('[')
		separator = ';'
	case OutFormatDebug:
		b.WriteString("record[")
	}

	first := true
	it := zv.Iter()
	for _, col := range t.Columns {
		val, _, err := it.Next()
		if err != nil {
			//XXX
			b.WriteString("ERR")
			break
		}
		if first {
			first = false
		} else {
			b.WriteByte(separator)
		}
		if val == nil {
			b.WriteByte('-')
		} else {
			b.WriteString(col.Type.StringOf(val, fmt, false))
		}
	}

	switch fmt {
	case OutFormatZNG:
		if !first {
			b.WriteByte(';')
		}
		b.WriteByte(']')
	case OutFormatDebug:
		b.WriteByte(']')
	}
	return b.String()
}

func (t *TypeRecord) Marshal(zv zcode.Bytes) (interface{}, error) {
	m := make(map[string]Value)
	it := zv.Iter()
	for _, col := range t.Columns {
		zv, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		m[col.Name] = Value{col.Type, zv}
	}
	return m, nil
}

func (t *TypeRecord) ColumnOfField(field string) (int, bool) {
	v, ok := t.LUT[field]
	return v, ok
}

func (t *TypeRecord) TypeOfField(field string) (Type, bool) {
	n, ok := t.LUT[field]
	if !ok {
		return nil, false
	}
	return t.Columns[n].Type, true
}

func (t *TypeRecord) HasField(field string) bool {
	_, ok := t.LUT[field]
	return ok
}

func (t *TypeRecord) createLUT() {
	t.LUT = make(map[string]int)
	for k, col := range t.Columns {
		t.LUT[col.Name] = k
	}
}
