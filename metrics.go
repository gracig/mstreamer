package mstreamer

import (
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
)

// Feedback takes a format string and a list of interfaces to  assemble a string.
// It was create to receive feedbacks (info, error messages) from other components in a centralized way
type Feedback func(format string, a ...interface{})

// Runnable takes a Feedback function and runs whatever logic inside and returns an error if any
type Runnable func(Feedback) error

// IOPipeline takes an input, a filter and an output
type IOPipeline func(Input, Filter, Output) (Runnable, error)

// Input takes a Feedback and returns a reader
type Input func(Feedback) (MeasureReader, error)

// FilteredInput composes a Input and a Filter and returns an Input function
type FilteredInput func(Input, Filter) (Input, error)

// MergedInput takes a list of inputs and merges it in a single input
type MergedInput func(...Input) (Input, error)

// ComposedInput composes a Source and Encoder and returns an Input function
type ComposedInput func(Source, Encoder) (Input, error)

// Source takes a Feedback function, get data from whatever source and returns that data in a io.ReadCloser object
// Initialization errors should be returned using the error object
// Runtime errors should be send to the Feedback function.
type Source func(Feedback) (io.ReadCloser, error)

// Encoder takes a Feedback function and a stream reader and returns a measure Reader and an error if any
// Encoder should read from the received stream and write encoded metrics into a Writer function.
// The data will be available to other systems through the returned Reader
// Initialization errors should be returned using the error object
// Runtime errors should be send to the Feedback function.
type Encoder func(Feedback, io.ReadCloser) (MeasureReader, error)

// Filter takes a Feedback funtion and a Measure Reader function and returns anoter Measure Reader funtion
// Filter should read measures from the received Measure Read, apply some kind of filter logic and
// write the results down to the output Reader
// Initialization errors should be returned using the error object
// Runtime errors should be send to the Feedback function.
type Filter func(Feedback, MeasureReader) (MeasureReader, error)

// ComposedFilter composes a set of filters in a chain
type ComposedFilter func(flts ...Filter) (Filter, error)

// Output takes a Feedback and a Reader and outputs to whatever place
type Output func(Feedback, MeasureReader) error

// FilteredOutput composes a Input and a Filter and returns an Input function
type FilteredOutput func(Filter, Output) (Output, error)

// MergedOutput takes a list of outputs and returns a single output function
type MergedOutput func(...Output) (Output, error)

// ComposedOutput composes a Source and Encoder and returns an Input function
type ComposedOutput func(Decoder, Sinker) (Output, error)

// Decoder takes a Feedback function and a Measure Reader and returns a generic reader and an error if any
// Decoder should read measures from the receiver Measure Read, transform it and
// write  the results down to the returned stream
// Initialization errors should be returned using the error object
// Runtime errors should be send to the Feedback function.
type Decoder func(Feedback, MeasureReader) (io.ReadCloser, error)

// Sinker takes a Feedback function and a generic Reader and returns an error if any
// Sinker should send the received data to other systems
// Initialization errors should be returned using the error object
// Runtime errors should be send to the Feedback function.
type Sinker func(Feedback, io.ReadCloser) error

// MeasureWriter takes a Measure struct, send it to any kind of transportation and return am error if any
type MeasureWriter interface {
	Write(Measure) error
}

// MeasureReader takes a Measure pointer, modifies it and return error if any
type MeasureReader interface {
	Read(*Measure) error
}

// Measure is the single unit of data we pass through systems
type Measure struct {
	Name string  `json:"name"`
	Tags []Tag   `json:"tags,omitempty"`
	Flds []Field `json:"flds"`
	Time int64   `json:"time"`
}

// Tag is the indexed portion of data. systems should index it to speed lookup searches
type Tag struct {
	Name string `json:"name,omitempty"`
	Data string `json:"data,omitempty"`
}

// Field is the non-indexed portion of a measure where one can input metrics
type Field struct {
	Name string      `json:"name,omitempty"`
	Type FieldType   `json:"type,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

// FieldType represents the type of a field data
type FieldType byte

const (
	//TBool is a boolean
	TBool = 'b'
	//TInt is an integer
	TInt = 'i'
	//TUint is an unsigned integer
	TUint = 'u'
	//TFloat is a float
	TFloat = 'f'
	//TString is a string
	TString = 's'
	//TNil is a nil value
	TNil = 'n'
)

// FieldValue returns the value of a field
func (m *Measure) FieldValue(name string) (interface{}, error) {
	f, err := m.Field(name)
	if err != nil {
		return nil, err
	}
	return f.Data, nil
}

// Field returns the field
func (m *Measure) Field(name string) (Field, error) {
	for _, f := range m.Flds {
		if f.Name == name {
			return f, nil
		}
	}
	return Field{}, errors.New("Field not found")
}

// TagValue returns the value of a tag
func (m *Measure) TagValue(name string) (string, error) {
	t, err := m.Tag(name)
	if err != nil {
		return "", err
	}
	return t.Data, nil
}

// Tag returns the tag
func (m *Measure) Tag(name string) (Tag, error) {
	for _, t := range m.Tags {
		if t.Name == name {
			return t, nil
		}
	}
	return Tag{}, errors.New("Tag not found")
}

// MakeTag creates a tag from a string
func MakeTag(name, value string) Tag {
	return Tag{Name: name, Data: value}
}

// ParseField parse a field from string
func ParseField(name, kind, value string) Field {
	t := ParseType(kind)
	v := ParseValue(t, value)
	return Field{Name: name, Type: t, Data: v}
}

// FieldValueType returns the type of a field
func FieldValueType(value interface{}) FieldType {
	switch value.(type) {
	case float64:
		return TFloat
	case int64:
		return TInt
	case uint64:
		return TUint
	case bool:
		return TBool
	case string:
		return TString
	default:
		return TNil
	}
}

// ParseValue parse a value from a FieldType
func ParseValue(t FieldType, value string) interface{} {
	switch t {
	case TFloat:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			v = math.NaN()
		}
		return v
	case TUint:
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			v = 0
		}
		return v
	case TInt:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			v = 0
		}
		return v
	case TBool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			v = false
		}
		return v
	case TString:
		return value
	case TNil:
		return nil
	default:
		return value
	}
}

// ParseType tries to convert string into types
func ParseType(str string) FieldType {
	switch str {
	case "string", "s", "text":
		return TString
	case "number", "int", "float", "u", "i", "f":
		return TFloat
	case "b", "boolean", "bool":
		return TBool
	default:
		return TString
	}
}

// SHA1 calculates a sha1 hash from this measure tags
func (m *Measure) SHA1() []byte {
	h := sha1Pool.Get().(hash.Hash)
	defer func() {
		h.Reset()
		sha1Pool.Put(h)
	}()
	for _, tag := range m.Tags {
		h.Write([]byte(tag.Data))
	}
	return h.Sum(nil)
}

var sha1Pool = sync.Pool{
	New: func() interface{} {
		return sha1.New()
	},
}

// MD5 calculates a md5 hash from this measure tags
func (m *Measure) MD5() []byte {
	h := md5Pool.Get().(hash.Hash)
	defer func() {
		h.Reset()
		md5Pool.Put(h)
	}()
	for _, tag := range m.Tags {
		h.Write([]byte(tag.Data))
	}
	return h.Sum(nil)
}

var md5Pool = sync.Pool{
	New: func() interface{} {
		return md5.New()
	},
}

// Compare returns -1 to less 0 to equal and +1 to more or error if note comparable
func (f *Field) Compare(o Field) (int, error) {
	if f.Type != o.Type {
		return 0, errors.New("cant compare fields of different types")
	}
	switch f.Type {
	case TFloat:
		if f.Data.(float64) < o.Data.(float64) {
			return -1, nil
		}
		if f.Data.(float64) > o.Data.(float64) {
			return +1, nil
		}
		return 0, nil
	case TString:
		return strings.Compare(f.Data.(string), o.Data.(string)), nil
	case TUint:
		if f.Data.(uint64) < o.Data.(uint64) {
			return -1, nil
		}
		if f.Data.(uint64) > o.Data.(uint64) {
			return +1, nil
		}
		return 0, nil
	case TInt:
		if f.Data.(int64) < o.Data.(int64) {
			return -1, nil
		}
		if f.Data.(int64) > o.Data.(int64) {
			return +1, nil
		}
		return 0, nil
	case TBool:
		if f.Data.(bool) == o.Data.(bool) {
			return 0, nil
		}
		if !f.Data.(bool) {
			return -1, nil
		}
		return +1, nil
	case TNil:
		return 0, errors.New("Cant compare nil values")
	default:
	}
	return 0, fmt.Errorf("Could not evaluate expression %v < %v", f.Data, o.Data)
}

// TagFeedback takes a tag key and prints it value
//func (m *Measure) TagFeedback(f Feedback, tag string) {
//
//	val, err := m.TagValue(tag)
//	if err != nil {
//		f(`m.TagValue(%s): does not exist`, tag)
//	}
//
//	f(`m.TagValue(%s): %s`, tag, val)
//
//}
