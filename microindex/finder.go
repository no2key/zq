package microindex

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimsec/zq/expr"
	"github.com/brimsec/zq/pkg/iosrc"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

var ErrNotFound = errors.New("key not found")

// Finder looks up values in a microindex using its embedded index.
type Finder struct {
	*Reader
	zctx    *resolver.Context
	uri     iosrc.URI
	builder *zng.Builder
}

// NewFinder returns an object that is used to lookup keys in a microindex.
func NewFinder(zctx *resolver.Context, uri iosrc.URI) *Finder {
	return &Finder{
		zctx: zctx,
		uri:  uri,
	}
}

// Open prepares the underlying microindex for lookups.  It opens the file
// and reads the trailer, returning errors if the file is corrrupt, doesn't
// exist, or its microindex trailer is invalid.  If the microindex exists
// but is empty, zero values are returned for any lookups.  If the microindex
// does not exist, os.ErrNotExist is returned.
func (f *Finder) Open() error {
	reader, err := NewReaderFromURI(f.zctx, f.uri)
	f.Reader = reader
	return err
}

// lookup searches for a match of the given key compared to the
// key values in the records read from the reader.  If the boolean argument
// "exact" is true, then only exact matches are returned.  Otherwise, the
// record with the lagest key smaller than the key argument is returned.
func lookup(reader zbuf.Reader, compare expr.KeyCompareFn, exact bool) (*zng.Record, error) {
	var prev *zng.Record
	for {
		rec, err := reader.Read()
		if err != nil || rec == nil {
			if exact {
				prev = nil
			}
			return prev, err
		}
		if cmp := compare(rec); cmp >= 0 {
			if cmp == 0 {
				return rec, nil
			}
			if exact {
				return nil, nil
			}
			return prev, nil
		}
		prev = rec
	}
}

func (f *Finder) search(compare expr.KeyCompareFn) (zbuf.Reader, error) {
	if f.reader == nil {
		panic("finder hasn't been opened")
	}
	// We start with the topmost level of the microindex file and
	// find the first key that matches according to the comparison,
	// then repeat the process for that frame in the next index file
	// till we get to the base layer and return a reader positioned at
	// that offset.
	n := len(f.trailer.Sections)
	off := int64(0)
	for level := 1; level < n; level++ {
		reader, err := f.newSectionReader(level, off)
		rec, err := lookup(reader, compare, false)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			// This key can't be in the microindex since it is
			// smaller than the smallest key present.
			return nil, ErrNotFound
		}
		off, err = rec.AccessInt(f.trailer.ChildOffsetField)
		if err != nil {
			return nil, fmt.Errorf("b-tree child field: %w", err)
		}
	}
	return f.newSectionReader(0, off)
}

func (f *Finder) Lookup(keys *zng.Record) (*zng.Record, error) {
	if f.IsEmpty() {
		return nil, nil
	}
	compare, err := expr.NewKeyCompareFn(keys)
	if err != nil {
		return nil, err
	}
	reader, err := f.search(compare)
	if err != nil {
		if err == ErrNotFound {
			// Return nil/success when exact-match lookup fails
			err = nil
		}
		return nil, err
	}
	return lookup(reader, compare, true)
}

func (f *Finder) LookupAll(ctx context.Context, hits chan<- *zng.Record, keys *zng.Record) error {
	if f.IsEmpty() {
		return nil
	}
	compare, err := expr.NewKeyCompareFn(keys)
	if err != nil {
		return err
	}
	reader, err := f.search(compare)
	if err != nil {
		return err
	}
	for {
		// As long as we have an exact key-match, where unset key
		// columns are "don't care", keep reading records and return
		// them via the channel.
		rec, err := lookup(reader, compare, true)
		if err != nil {
			return err
		}
		if rec == nil {
			return nil
		}
		select {
		case hits <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *Finder) LookupClosest(keys *zng.Record) (*zng.Record, error) {
	if f.IsEmpty() {
		return nil, nil
	}
	compare, err := expr.NewKeyCompareFn(keys)
	if err != nil {
		return nil, err
	}
	reader, err := f.search(compare)
	if err != nil {
		return nil, err
	}
	return lookup(reader, compare, false)
}

// ParseKeys uses the key template from the microindex trailer to parse
// a slice of string values which correspnod to the DFS-order
// of the fields in the key.  The inputs may be smaller than the
// number of key fields, in which case they are "don't cares"
// in terms of key lookups.  Any don't-care fields must all be
// at the end of the key record.
func (f *Finder) ParseKeys(inputs []string) (*zng.Record, error) {
	if f.IsEmpty() {
		return nil, nil
	}
	if f.builder == nil {
		f.builder = zng.NewBuilder(f.trailer.KeyType)
	}
	rec, err := f.builder.Parse(inputs...)
	if err == zng.ErrIncomplete {
		err = nil
	}
	return rec, err
}
