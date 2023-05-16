package mstreamer

import (
	"errors"
	"io"
)

// DecoderAdapter takes
type DecoderAdapter func(Feedback, MeasureReader, io.Writer)

// DecoderToWriter das
type DecoderToWriter func(Measure, io.Writer) error

// NewGenericDecoder is
func NewGenericDecoder(decw DecoderToWriter) (Decoder, error) {
	return newGenericDecoder(decw)
}

// NewDecoder takes
func NewDecoder(adapter DecoderAdapter) (Decoder, error) {
	return newDecoder(adapter, io.Pipe)
}
func newDecoder(
	adapter DecoderAdapter,
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
) (Decoder, error) {
	if adapter == nil {
		return nil, errors.New("adapter function is nil")
	}
	if ioPipe == nil {
		return nil, errors.New("iopipe is nil")
	}
	return func(f Feedback, r MeasureReader) (io.ReadCloser, error) {
		if f == nil {
			return nil, errors.New("feedback funcion is nil")
		}
		if r == nil {
			return nil, errors.New("reader stream is nil")
		}
		pr, pw := ioPipe()
		go func() {
			defer pw.Close()
			adapter(f, r, pw)
		}()
		return pr, nil
	}, nil
}

func newGenericDecoder(decw DecoderToWriter) (Decoder, error) {
	if decw == nil {
		return nil, errors.New("decoder function is nil")
	}
	adapter := func(f Feedback, r MeasureReader, w io.Writer) {
		for {
			var measure Measure
			err := r.Read(&measure)
			if err != nil {
				if err == io.EOF {
					break
				}
				f("genericDecoder read error- %v", err)
				continue
			}
			err = decw(measure, w)
			if err != nil {
				f("genericDecoder decode error- %v", err)
				continue
			}
		}
	}
	return NewDecoder(adapter)
}
