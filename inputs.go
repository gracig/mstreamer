package mstreamer

import (
	"errors"
	"io"
	"sync"
)

// NewInputFromProducer takes a producer function and returns an Input function
func NewInputFromProducer(producer func(f Feedback, w MeasureWriter)) (Input, error) {
	return newInputFromProducer(producer, io.Pipe, NewWriter, NewReader)
}

// NewMergedInput takes a list of inputs and merges it in a single input
func NewMergedInput(inputs ...Input) (Input, error) {
	return newMergedInput(io.Pipe, NewWriter, NewReader, inputs...)
}

// NewComposedInput composes a source and a encoder
func NewComposedInput(src Source, enc Encoder) (Input, error) {
	if src == nil {
		return nil, errors.New("source function is nil")
	}
	if enc == nil {
		return nil, errors.New("encode function is nil")
	}

	return func(f Feedback) (MeasureReader, error) {
		rsrc, err := src(f)
		if err != nil {
			return nil, err
		}
		return enc(f, rsrc)
	}, nil
}

// NewFilteredInput composes a Input and a Filter and returns an Input function
func NewFilteredInput(inp Input, flt Filter) (Input, error) {
	if inp == nil {
		return nil, errors.New("input function is nil")
	}
	if flt == nil {
		return nil, errors.New("filter function is nil")
	}
	return func(f Feedback) (MeasureReader, error) {
		rinp, err := inp(f)
		if err != nil {
			return nil, err
		}
		return flt(f, rinp)
	}, nil
}

func newInputFromProducer(
	producer func(f Feedback, w MeasureWriter),
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
	mWriter func(io.Writer) MeasureWriter,
	mReader func(io.Reader) MeasureReader,
) (Input, error) {
	if producer == nil {
		return nil, errors.New("Input producer function is nil")
	}
	if ioPipe == nil {
		return nil, errors.New("ioPipe is nil")
	}
	if mWriter == nil {
		return nil, errors.New("ioWriter is nil")
	}
	if mReader == nil {
		return nil, errors.New("ioReader is nil")
	}
	return func(f Feedback) (MeasureReader, error) {
		if f == nil {
			return nil, errors.New("Feedback function is nil")
		}
		pr, pw := ioPipe()
		mr := mReader(pr)
		mw := mWriter(pw)
		go func() {
			defer pw.Close()
			producer(f, mw)
		}()
		return mr, nil
	}, nil
}

// newMergedInput takes a list of inputs and merges it in a single input
func newMergedInput(
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
	mWriter func(io.Writer) MeasureWriter,
	mReader func(io.Reader) MeasureReader,
	inputs ...Input,
) (Input, error) {
	if ioPipe == nil {
		return nil, errors.New("iopipe is nil")
	}
	if mWriter == nil {
		return nil, errors.New("mWriter is nil")
	}
	if mReader == nil {
		return nil, errors.New("mReader is nil")
	}

	return func(f Feedback) (MeasureReader, error) {
		if f == nil {
			return nil, errors.New("feedback funcion is nil")
		}
		var wg sync.WaitGroup
		outc := make(chan Measure)
		//FanOut
		for _, input := range inputs {
			r, err := input(f)
			if err != nil {
				return nil, err
			}
			go func(r MeasureReader) {
				for {
					var m Measure
					err := r.Read(&m)
					if err != nil {
						if err == io.EOF {
							break
						}
						f("error on reading measure %v", err)
					}
					outc <- m
				}
				wg.Done()
			}(r)
			wg.Add(1)
		}
		go func() {
			wg.Wait()
			close(outc)
		}()

		pr, pw := ioPipe()
		mr := mReader(pr)
		mw := mWriter(pw)
		//FanIn
		go func() {
			defer pw.Close()
			for m := range outc {
				mw.Write(m)
			}
		}()

		return mr, nil
	}, nil
}
