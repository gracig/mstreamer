package mstreamer

import (
	"encoding/gob"
	"io"
)

// NewWriter sends measures over io.Writer
func NewWriter(w io.Writer) MeasureWriter {
	return &encoder{encoder: gob.NewEncoder(w)}
}

// NewReader read measures from io.Reader
func NewReader(r io.Reader) MeasureReader {
	return &decoder{decoder: gob.NewDecoder(r)}
}

type encoder struct {
	encoder *gob.Encoder
}

func (e *encoder) Write(d Measure) error {
	return e.encoder.Encode(d)
}

type decoder struct {
	decoder *gob.Decoder
}

func (e *decoder) Read(d *Measure) error {
	return e.decoder.Decode(d)
}
