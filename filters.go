package mstreamer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
)

// FilterAdapter takes a Feedback, a Measure pointer and a MeasureWriter and
// Create a filter using Measure as input
// Write down filtered Measure information on MeasureWriter
// Use Feedback funtion to inform about any errors or debug information
type FilterAdapter func(Feedback, *Measure, MeasureWriter)

// FinalizeAdapter is a function called when filter stream has been closed
type FinalizeAdapter func(Feedback, MeasureWriter)

// NewFilter takes and adapter that implements filter logic and returns a brand new filter
// to be used in pipelines
func NewFilter(adapter FilterAdapter, finalizer FinalizeAdapter) (Filter, error) {
	return newFilter(adapter, finalizer, io.Pipe, NewWriter, NewReader)
}

// NewComposedFilter takes a list of Filters and returns a new Filter that is a composition of all
// input filters running sequentially from left to right
func NewComposedFilter(flts ...Filter) (Filter, error) {
	return newComposedFilter(flts...)
}

// NewTimeInjectorFilter takes a time and returns a Filter that inject that time on every measure received
func NewTimeInjectorFilter(time int64) (Filter, error) {
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			m.Time = time
			mw.Write(*m)
		}, nil)
}

// NewNameInjectorFilter takes a name and returns a Filter that inject that name on every measure received
func NewNameInjectorFilter(name string) (Filter, error) {
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			m.Name = name
			mw.Write(*m)
		}, nil)
}

// NewNameSanityFilter takes a name and returns a Filter that inject that name on every measure received
func NewNameSanityFilter() (Filter, error) {
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			m.Name = strings.ReplaceAll(m.Name, " ", "")
			m.Name = strings.ToLower(m.Name)
			mw.Write(*m)
		}, nil)
}

// NewMeasureCountFilter counts the number of inputed measures
func NewMeasureCountFilter() (Filter, error) {
	counter := make(map[string]uint64)
	finalizer := func(f Feedback, mw MeasureWriter) {
		var keys []string
		for k := range counter {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			f("Total %v records: %v", k, counter[k])
		}
	}
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			if _, ok := counter[m.Name]; !ok {
				counter[m.Name] = 0
			}
			counter[m.Name]++
			mw.Write(*m)
		}, finalizer)
}

// NewByPassFilter takes a name and returns a Filter that inject that name on every measure received
func NewByPassFilter() (Filter, error) {
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			mw.Write(*m)
		}, nil)
}

// NewTagInjectorFilter takes a list of tags and returns a Filter that inject those tags on every measure received
func NewTagInjectorFilter(tags ...Tag) (Filter, error) {
	if tags == nil {
		return nil, errors.New("Tag list is nil")
	}
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			m.Tags = append(m.Tags, tags...)
			mw.Write(*m)
		}, nil)
}

// NewFieldInjectorFilter takes a list of fields and returnas a filter that inject those fields on every measure received
func NewFieldInjectorFilter(fields ...Field) (Filter, error) {
	if fields == nil {
		return nil, errors.New("Field list is nil")
	}
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			m.Flds = append(m.Flds, fields...)
			mw.Write(*m)
		}, nil)
}

// NewLogFilter takes a label and returns a Filters that prints out every Measure, tagged with the label,  on console output
func NewLogFilter(label string) (Filter, error) {
	return NewFilter(
		func(f Feedback, m *Measure, mw MeasureWriter) {
			b, err := json.MarshalIndent(m, "", "  ")
			if err != nil {
				fmt.Println("error:", err)
			}
			log.Printf("%v: %s", label, b)
			mw.Write(*m)
		}, nil)
}

// newFilter receives a filterAdapter with the filter logic and returns a new filter to be used in pipelines
func newFilter(
	adapter FilterAdapter,
	finalizer FinalizeAdapter,
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
	mWriter func(io.Writer) MeasureWriter,
	mReader func(io.Reader) MeasureReader,
) (Filter, error) {
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
	return func(f Feedback, r MeasureReader) (MeasureReader, error) {
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
			defer pw.Close()
			for {
				var m Measure
				err := r.Read(&m)
				if err != nil {
					if err == io.EOF {
						if finalizer != nil {
							finalizer(f, mw)
						}
						break
					}
					f("applierFilter error on read: %v", err)
					continue
				}
				adapter(f, &m, mw)
			}
		}()
		return mr, nil
	}, nil
}

func newComposedFilter(flts ...Filter) (Filter, error) {
	if flts == nil || flts[0] == nil {
		log.Printf("no filters")
		//		return nil, errors.New("")
	}
	return func(f Feedback, r MeasureReader) (MeasureReader, error) {
		for _, flt := range flts {
			fr, err := flt(f, r)
			if err != nil {
				return nil, err
			}
			r = fr
		}
		return r, nil
	}, nil
}
