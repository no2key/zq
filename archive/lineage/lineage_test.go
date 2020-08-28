package lineage

import (
	"strconv"
	"testing"

	"github.com/brimsec/zq/pkg/nano"
	"github.com/stretchr/testify/assert"
)

func TestReadPlan(t *testing.T) {
	cases := []struct {
		chunks []Chunk
		exp    []Segment
	}{
		{
			chunks: []Chunk{
				{Id: "a", Span: nano.Span{Ts: 0, Dur: 4}},
				{Id: "b", Span: nano.Span{Ts: 2, Dur: 5}},
				{Id: "ab", Span: nano.Span{Ts: 2, Dur: 2}, Ancestors: []string{"a", "b"}},
				{Id: "c", Span: nano.Span{Ts: 2, Dur: 2}},
			},
			exp: []Segment{
				{Ids: []string{"a"}, Span: nano.Span{Ts: 0, Dur: 2}},
				{Ids: []string{"ab", "c"}, Span: nano.Span{Ts: 2, Dur: 2}},
				{Ids: []string{"b"}, Span: nano.Span{Ts: 4, Dur: 3}},
			},
		},
		{
			chunks: nil,
			exp:    nil,
		},
	}
	for i := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, cases[i].exp, ReadPlan(cases[i].chunks))
		})
	}
}

func TestCalcSegs(t *testing.T) {
	cases := []struct {
		chunks []Chunk
		exp    []Segment
	}{
		{
			chunks: []Chunk{
				{Id: "a", Span: nano.Span{Ts: 0, Dur: 1}},
				{Id: "b", Span: nano.Span{Ts: 1, Dur: 1}},
			},
			exp: []Segment{
				{Ids: []string{"a"}, Span: nano.Span{Ts: 0, Dur: 1}},
				{Ids: []string{"b"}, Span: nano.Span{Ts: 1, Dur: 1}},
			},
		},
		{
			chunks: []Chunk{
				{Id: "a", Span: nano.Span{Ts: 0, Dur: 2}},
				{Id: "b", Span: nano.Span{Ts: 1, Dur: 2}},
			},
			exp: []Segment{
				{Ids: []string{"a"}, Span: nano.Span{Ts: 0, Dur: 1}},
				{Ids: []string{"a", "b"}, Span: nano.Span{Ts: 1, Dur: 1}},
				{Ids: []string{"b"}, Span: nano.Span{Ts: 2, Dur: 1}},
			},
		},
		{
			chunks: []Chunk{
				{Id: "a", Span: nano.Span{Ts: 0, Dur: 4}},
				{Id: "b", Span: nano.Span{Ts: 1, Dur: 2}},
			},
			exp: []Segment{
				{Ids: []string{"a"}, Span: nano.Span{Ts: 0, Dur: 1}},
				{Ids: []string{"a", "b"}, Span: nano.Span{Ts: 1, Dur: 2}},
				{Ids: []string{"a"}, Span: nano.Span{Ts: 3, Dur: 1}},
			},
		},
		{
			chunks: nil,
			exp:    nil,
		},
	}
	for i := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, cases[i].exp, calcSegs(cases[i].chunks))
		})
	}
}
