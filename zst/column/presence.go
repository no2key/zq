package column

type PresenceWriter struct {
	IntWriter
	n     int32
	unset bool
}

func NewPresenceWriter(spiller *Spiller) *PresenceWriter {
	return &PresenceWriter{
		IntWriter: *NewIntWriter(spiller),
	}
}

func (p *PresenceWriter) TouchValue() {
	if !p.unset {
		p.n++
	} else {
		p.Write(p.n)
		p.n = 1
		p.unset = false
	}
}

func (p *PresenceWriter) TouchUnset() {
	if p.unset {
		p.n++
	} else {
		p.Write(p.n)
		p.n = 1
		p.unset = true
	}
}

func (p *PresenceWriter) Finish() {
	p.Write(p.n)
}
