package mstreamer

import (
	"encoding/json"
	"errors"
	"io"
)

// EncoderAdapter takes
type EncoderAdapter func(Feedback, io.Reader, MeasureWriter)

// EncodeToWriter das
type EncodeToWriter func(interface{}, MeasureWriter) error

// NewFromJSONEncoder is
func NewFromJSONEncoder(data interface{}, encode EncodeToWriter) (Encoder, error) {
	return newFromJSONEncoder(data, encode)
}

// NewEncoder builds an encoder function
func NewEncoder(adapter EncoderAdapter) (Encoder, error) {
	return newEncoder(adapter, io.Pipe, NewWriter, NewReader)
}

func newFromJSONEncoder(data interface{}, encode EncodeToWriter) (Encoder, error) {
	adapter := func(f Feedback, r io.Reader, w MeasureWriter) {
		if err := json.NewDecoder(r).Decode(data); err != nil {
			f("error decoding json %v", err)
		}
		encode(data, w)
	}
	return NewEncoder(adapter)
}

func newEncoder(
	adapter EncoderAdapter,
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
	mWriter func(io.Writer) MeasureWriter,
	mReader func(io.Reader) MeasureReader,
) (Encoder, error) {
	if adapter == nil {
		return nil, errors.New("adapter function is nil")
	}
	if ioPipe == nil {
		return nil, errors.New("iopipe is nil")
	}
	if mWriter == nil {
		return nil, errors.New("mWriter is nil")
	}
	if mReader == nil {
		return nil, errors.New("mReader is nil")
	}
	return func(f Feedback, r io.ReadCloser) (MeasureReader, error) {
		if f == nil {
			return nil, errors.New("feedback funcion is nil")
		}
		if r == nil {
			return nil, errors.New("reader stream is nil")
		}
		pr, pw := ioPipe()
		mr := mReader(pr)
		mw := mWriter(pw)
		go func() {
			defer r.Close()
			defer pw.Close()
			adapter(f, r, mw)
		}()
		return mr, nil
	}, nil
}
