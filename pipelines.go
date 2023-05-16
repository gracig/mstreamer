package mstreamer

import (
	"errors"
)

// NewIOPipeline takes one component of each simple type
func NewIOPipeline(i Input, f Filter, o Output) (Runnable, error) {
	if i == nil {
		return nil, errors.New("input function is nil")
	}
	if f == nil {
		return nil, errors.New("filter function is nil")
	}
	if o == nil {
		return nil, errors.New("output function is nil")
	}
	return func(fb Feedback) error {
		if fb == nil {
			return errors.New("feedback function is nil")
		}
		ri, err := i(fb)
		if err != nil {
			return err
		}
		rf, err := f(fb, ri)
		if err != nil {
			return err
		}
		return o(fb, rf)
	}, nil
}
