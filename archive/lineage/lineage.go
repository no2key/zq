package lineage

import (
	"sort"

	"github.com/brimsec/zq/pkg/nano"
)

type Chunk struct {
	Id        string
	Span      nano.Span
	Ancestors []string
}

type Segment struct {
	Ids  []string
	Span nano.Span
}

func ReadPlan(chunks []Chunk) []Segment {
	m := make(map[string]Chunk, len(chunks))
	for _, c := range chunks {
		m[c.Id] = c
	}
	segs := calcSegs(chunks)
	for i := range segs {
		var ancestors []string
		for _, a := range segs[i].Ids {
			ancestors = append(ancestors, m[a].Ancestors...)
		}
		segs[i].Ids = copyExcept(segs[i].Ids, ancestors)
	}
	return segs
}

func calcSegs(chunks []Chunk) []Segment {
	var segIds []string
	var segStart nano.Ts
	var segs []Segment
	boundaries(chunks, func(ts nano.Ts, startIds, endIds []string) {
		if len(startIds) > 0 {
			if len(segIds) > 0 {
				segs = append(segs, Segment{
					Ids: copyExcept(segIds, nil),
					Span: nano.Span{
						Ts:  segStart,
						Dur: int64(ts - segStart),
					},
				})
			}
			segIds = append(segIds, startIds...)
			segStart = ts
		}
		if len(endIds) > 0 {
			segs = append(segs, Segment{
				Ids: copyExcept(segIds, nil),
				Span: nano.Span{
					Ts:  segStart,
					Dur: int64(ts - segStart + 1),
				},
			})
			segIds = copyExcept(segIds, endIds)
			segStart = ts + 1
		}
	})
	return segs
}

func copyExcept(src []string, skip []string) (dst []string) {
outer:
	for i := range src {
		for j := range skip {
			if src[i] == skip[j] {
				continue outer
			}
		}
		dst = append(dst, src[i])
	}
	return
}

type point struct {
	id    string
	start bool
	ts    nano.Ts
}
type points []point

func (p points) Len() int           { return len(p) }
func (p points) Less(i, j int) bool { return p[i].ts < p[j].ts }
func (p points) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func boundaries(chunks []Chunk, fn func(ts nano.Ts, startIds, endIds []string)) {
	points := make(points, 0, 2*len(chunks))
	for _, c := range chunks {
		points = append(points, point{id: c.Id, start: true, ts: c.Span.Ts})
		points = append(points, point{id: c.Id, ts: nano.Ts(int64(c.Span.Ts) + c.Span.Dur - 1)})
	}
	sort.Sort(points)
	var startIds, endIds []string
	for i := 0; i < len(points); {
		j := i + 1
		for ; j < len(points); j++ {
			if points[i].ts != points[j].ts {
				break
			}
		}
		startIds = startIds[:0]
		endIds = endIds[:0]
		for _, p := range points[i:j] {
			if p.start {
				startIds = append(startIds, p.id)
			} else {
				endIds = append(endIds, p.id)
			}
		}
		ts := points[i].ts
		i = j
		fn(ts, startIds, endIds)
	}
}
