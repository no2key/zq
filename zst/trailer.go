package zst

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/brimsec/zq/zcode"
	"github.com/brimsec/zq/zio/zngio"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/resolver"
)

const (
	MagicField      = "magic"
	VersionField    = "version"
	SkewThreshField = "skew_thresh"
	ColThreshField  = "col_thresh"
	SectionsField   = "sections"

	MagicVal   = "zst"
	VersionVal = 1

	TrailerMaxSize = 4096
)

// XXX we should make generic trailer package and share between microindex and zst

type Trailer struct {
	Magic      string
	Version    int
	SkewThresh int
	ColThresh  int
	Sections   []int64
}

var ErrNotZst = errors.New("not a zst object")

func newTrailerRecord(zctx *resolver.Context, skewThresh, colThresh int, sections []int64) (*zng.Record, error) {
	sectionsType := zctx.LookupTypeArray(zng.TypeInt64)
	cols := []zng.Column{
		{MagicField, zng.TypeString},
		{VersionField, zng.TypeInt32},
		{SkewThreshField, zng.TypeInt32},
		{ColThreshField, zng.TypeInt32},
		{SectionsField, sectionsType},
	}
	typ, err := zctx.LookupTypeRecord(cols)
	if err != nil {
		return nil, err
	}
	builder := zng.NewBuilder(typ)
	return builder.Build(
		zng.EncodeString(MagicVal),
		zng.EncodeInt(VersionVal),
		zng.EncodeInt(int64(skewThresh)),
		zng.EncodeInt(int64(colThresh)),
		encodeSections(sections)), nil
}

func encodeSections(sections []int64) zcode.Bytes {
	var b zcode.Builder
	for _, s := range sections {
		b.AppendPrimitive(zng.EncodeInt(s))
	}
	return b.Bytes()
}

func readTrailer(r io.ReadSeeker, n int64) (*Trailer, int, error) {
	if n > TrailerMaxSize {
		n = TrailerMaxSize
	}
	if _, err := r.Seek(-n, io.SeekEnd); err != nil {
		return nil, 0, err
	}
	buf := make([]byte, n)
	cc, err := r.Read(buf)
	if err != nil {
		return nil, 0, err
	}
	if int64(cc) != n {
		// this shouldn't happen but maybe could occur under a corner case
		// or I/O problems XXX
		return nil, 0, fmt.Errorf("couldn't read trailer: expected %d bytes but read %d", n, cc)
	}
	for off := int(n) - 3; off >= 0; off-- {
		// look for end of stream followed by an array[int64] typedef then
		// a record typedef indicating the possible presence of the trailer,
		// which we then try to decode.
		if bytes.Equal(buf[off:(off+3)], []byte{zng.TypeDefArray, zng.IdInt64, zng.TypeDefRecord}) {
			if off > 0 && buf[off-1] != zng.CtrlEOS {
				// If this isn't right after an end-of-stream
				// (and we're not at the start of index), then
				// we skip because it can't be a valid trailer.
				continue
			}
			r := bytes.NewReader(buf[off:n])
			rec, _ := zngio.NewReader(r, resolver.NewContext()).Read()
			if rec == nil {
				continue
			}
			_, err := trailerVersion(rec)
			if err != nil {
				return nil, 0, err
			}
			trailer, _ := recordToTrailer(rec)
			if trailer != nil {
				return trailer, int(n) - off, nil
			}
		}
	}
	return nil, 0, errors.New("zst trailer not found")
}

func trailerVersion(rec *zng.Record) (int, error) {
	version, err := rec.AccessInt(VersionField)
	if err != nil {
		return -1, errors.New("zst version field is not a valid int32")
	}
	if version != VersionVal {
		return -1, fmt.Errorf("zst version %d found while expecting version %d", version, VersionVal)
	}
	return int(version), nil
}

func recordToTrailer(rec *zng.Record) (*Trailer, error) {
	var trailer Trailer
	var err error
	trailer.Magic, err = rec.AccessString(MagicField)
	if err != nil || trailer.Magic != MagicVal {
		return nil, ErrNotZst
	}
	trailer.Version, err = trailerVersion(rec)
	if err != nil {
		return nil, err
	}

	trailer.Sections, err = decodeSections(rec)
	if err != nil {
		return nil, err
	}
	return &trailer, nil
}

func decodeSections(rec *zng.Record) ([]int64, error) {
	v, err := rec.Access(SectionsField)
	if err != nil {
		return nil, err
	}
	arrayType, ok := v.Type.(*zng.TypeArray)
	if !ok {
		return nil, fmt.Errorf("%s field in microindex trailer is not an arrray", SectionsField)
	}
	if v.Bytes == nil {
		// This is an empty index.  Just return nil/success.
		return nil, nil
	}
	zvals, err := arrayType.Decode(v.Bytes)
	if err != nil {
		return nil, err
	}
	var sizes []int64
	for _, zv := range zvals {
		if zv.Type != zng.TypeInt64 {
			return nil, errors.New("section element is not an int64")
		}
		size, err := zng.DecodeInt(zv.Bytes)
		if err != nil {
			return nil, errors.New("int64 section element could not be decoded")
		}
		sizes = append(sizes, size)
	}
	return sizes, nil
}
