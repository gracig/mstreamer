package mstreamer

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestNewIOPipeline(t *testing.T) {

	//those are the arguments for the NewIOPipeline function
	type args struct {
		i Input
		f Filter
		o Output
	}

	//Creates a measure to go throguh the streamer
	sm := Measure{
		"sample",
		[]Tag{
			{"app", "metrics"},
		},
		[]Field{
			{"refactorings", 'i', 4},
		},
		1257894000000000000,
	}

	//dargs contains the inputs, filter and output function for the IOPipeine
	dargs := args{
		i: func(f Feedback) (MeasureReader, error) {
			//This input is pipe. Here we create a pipe
			//pr : pipe reader
			//p : pipe writer
			pr, pw := io.Pipe()

			//A go function to write the measure on the pw
			go func() {
				defer pw.Close()
				mw := NewWriter(pw)
				mw.Write(sm)
			}()

			return NewReader(pr), nil
		},
		f: func(f Feedback, r MeasureReader) (MeasureReader, error) {
			if r == nil {
				return nil, errors.New("reader is nil")
			}

			var m Measure
			r.Read(&m)
			if !reflect.DeepEqual(m, sm) {
				return nil, fmt.Errorf("flt: measure does not match. got %#v want %#v", m, sm)
			}
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				mw := NewWriter(pw)
				mw.Write(sm)
			}()
			return NewReader(pr), nil
		},
		o: func(f Feedback, r MeasureReader) error {
			if r == nil {
				return errors.New("reader is nil")
			}
			var m Measure
			r.Read(&m)
			if !reflect.DeepEqual(m, sm) {
				return fmt.Errorf("dec: measure does not match. got %v want%v", m, sm)
			}
			return nil
		},
	}
	exec := func() func(Runnable) error {
		return func(p Runnable) error {
			return p(t.Logf)
		}
	}

	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantExecErr bool
		exec        func(Runnable) error
	}{
		{
			name: `when building a pipeline with mocked components, 
			then all components should be called and their data should pass`,
			wantErr: false, wantExecErr: false, args: dargs, exec: exec(),
		},
		{
			name:    `when input component generate error then should fail`,
			wantErr: false, wantExecErr: true, exec: exec(),
			args: func(a args) args {
				a.i = func(f Feedback) (MeasureReader, error) {
					return nil, errors.New("input error")
				}
				return a
			}(dargs),
		},
		{
			name:    `when filter component generate error then should fail`,
			wantErr: false, wantExecErr: true, exec: exec(),
			args: func(a args) args {
				a.f = func(Feedback, MeasureReader) (MeasureReader, error) {
					return nil, errors.New("filter error")
				}
				return a
			}(dargs),
		},
		{
			name:    `when output component generate error then should fail`,
			wantErr: false, wantExecErr: true, exec: exec(),
			args: func(a args) args {
				a.o = func(Feedback, MeasureReader) error {
					return errors.New("output error")
				}
				return a
			}(dargs),
		},
		{
			name: `when input is nil`, wantErr: true, wantExecErr: false, exec: exec(),
			args: func(a args) args { a.i = nil; return a }(dargs),
		},
		{
			name: `when filter is nil`, wantErr: true, wantExecErr: false, exec: exec(),
			args: func(a args) args { a.f = nil; return a }(dargs),
		},
		{
			name: `when output is nil`, wantErr: true, wantExecErr: false, exec: exec(),
			args: func(a args) args { a.o = nil; return a }(dargs),
		},
		{
			name: `when feedback is nil`, wantErr: false, wantExecErr: true, args: dargs,
			exec: func(p Runnable) error { return p(nil) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIOPipeline(tt.args.i, tt.args.f, tt.args.o)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIOPipeline() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && err == nil {
				t.Errorf("got nil functio but err was nil")
				return
			}
			if got != nil && tt.exec != nil {
				err = tt.exec(got)
				if (err != nil) != tt.wantExecErr {
					t.Errorf("NewPipeline() error = %v, wantExecErr %v", err, tt.wantExecErr)
					return
				}
			}
		})
	}
}
