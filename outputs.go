package mstreamer

import (
	"errors"
	"io"
	"sync"
)

// NewMergedOutput takes a list of outputs and returns a single output function
func NewMergedOutput(outputs ...Output) (Output, error) {
	if outputs == nil {
		return nil, errors.New("Merge outputs with an empty list does nothing")
	}
	return newMergedOutput(io.Pipe, NewWriter, NewReader, outputs...)
}

// NewFilteredOutput composes a Input and a Filter and returns an Input function
func NewFilteredOutput(flt Filter, out Output) (Output, error) {
	if flt == nil {
		return nil, errors.New("filter function is nil")
	}
	if out == nil {
		return nil, errors.New("output function is nil")
	}
	return func(f Feedback, r MeasureReader) error {
		rflt, err := flt(f, r)
		if err != nil {
			return err
		}
		return out(f, rflt)
	}, nil
}

// NewComposedOutput is
func NewComposedOutput(dec Decoder, snk Sinker) (Output, error) {
	if dec == nil {
		return nil, errors.New("decoder is nil")
	}
	if snk == nil {
		return nil, errors.New("sinker is nil")
	}
	return func(f Feedback, r MeasureReader) error {
		rdec, err := dec(f, r)
		if err != nil {
			return err
		}
		return snk(f, rdec)
	}, nil
}

// newMergedOutput takes a list of outputs and returns a single output function
func newMergedOutput(
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
	mWriter func(io.Writer) MeasureWriter,
	mReader func(io.Reader) MeasureReader,
	outputs ...Output) (Output, error) {
	if ioPipe == nil {
		return nil, errors.New("iopipe function is nil")
	}
	if mWriter == nil {
		return nil, errors.New("mWriter is nil")
	}
	if mReader == nil {
		return nil, errors.New("mReader is nil")
	}
	return func(f Feedback, r MeasureReader) error {

		var mws []MeasureWriter
		var pws []*io.PipeWriter
		var wg sync.WaitGroup
		for _, out := range outputs {
			pr, pw := ioPipe()
			mr := mReader(pr)
			mws = append(mws, mWriter(pw))
			pws = append(pws, pw)
			go func(o Output, mr MeasureReader) {
				defer wg.Done()
				err := o(f, mr)
				if err != nil {
					f("sink fail %v", err)
				}
			}(out, mr)
			wg.Add(1)
		}
		for {
			var m Measure
			err := r.Read(&m)
			if err != nil {
				if err == io.EOF {
					break
				}
				f("error reading message %v", m)
				continue
			}
			for _, mw := range mws {
				err = mw.Write(m)
				if err != nil {
					f("fail pushing metric %v", err)
				}
			}
		}
		for _, pw := range pws {
			pw.Close()
		}
		wg.Wait()
		return nil
	}, nil

}
