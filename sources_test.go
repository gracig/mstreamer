package mstreamer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func Test_newSource(t *testing.T) {
	type args struct {
		adapter SourceAdapter
		ioPipe  func() (*io.PipeReader, *io.PipeWriter)
	}
	ss := "sample"
	dargs := args{
		adapter: func(f Feedback, w io.Writer) {
			w.Write([]byte(ss))
		},
		ioPipe: io.Pipe,
	}
	if _, err := NewSource(dargs.adapter); err != nil {
		t.Errorf("error calling constructor")
	}
	exec := func(fok bool) func(Source) error {
		var f Feedback
		if fok {
			f = func(format string, a ...interface{}) {
				t.Errorf(format, a...)
			}
		}
		return func(src Source) error {
			r, err := src(f)
			if err != nil {
				return err
			}
			if r == nil {
				return errors.New("reader is nil")
			}
			var buf bytes.Buffer
			buf.ReadFrom(r)
			if buf.String() != ss {
				return fmt.Errorf("got %v want %v", buf.String(), ss)
			}
			return nil
		}
	}

	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantExecErr bool
		exec        func(Source) error
	}{
		{
			name:    `when provided with an adapter then should not fail`,
			wantErr: false, wantExecErr: false, args: dargs, exec: exec(true),
		},

		{
			name:    `when feedback is nil then should fail`,
			wantErr: false, wantExecErr: true, args: dargs, exec: exec(false),
		},
		{
			name:    `when ioPipe is nil then should fail`,
			wantErr: true, wantExecErr: false, exec: exec(true),
			args: func(a args) args { a.ioPipe = nil; return a }(dargs),
		},
		{
			name:    `when adapter is nil then should fail`,
			wantErr: true, wantExecErr: false, exec: exec(true),
			args: func(a args) args { a.adapter = nil; return a }(dargs),
		},
		{
			name:    `when adapter send a different string should fail`,
			wantErr: false, wantExecErr: true, exec: exec(true),
			args: func(a args) args {
				a.adapter = func(f Feedback, w io.Writer) {
					w.Write([]byte("wrong string"))
				}
				return a
			}(dargs),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newSource(tt.args.adapter, tt.args.ioPipe)
			if (err != nil) != tt.wantErr {
				t.Errorf("newSource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && err == nil {
				t.Errorf("newSource() error = got nil function but err was nil")
				return
			}
			if got != nil && tt.exec != nil {
				err = tt.exec(got)
				if (err != nil) != tt.wantExecErr {
					t.Errorf("newSource() error = %v, wantExecErr %v", err, tt.wantExecErr)
					return
				}
			}
		})
	}
}
