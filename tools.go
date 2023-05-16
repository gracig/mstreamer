package mstreamer

import "io"

// Helpers is a struct with Helper functions to be used inside components
type Helpers struct {
	IOPipe    func() (*io.PipeReader, *io.PipeWriter)
	IOcopy    func(io.Writer, io.Reader)
	NewWriter func(io.Writer) MeasureWriter
	NewReader func(io.Reader) MeasureReader
}
