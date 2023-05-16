package mstreamer

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

// SourceAdapter takes
type SourceAdapter func(Feedback, io.Writer)

// SourceGetter takes
type SourceGetter func() (io.ReadCloser, error)

// NewBasicHTTPSource is a basic HTTP Source
func NewBasicHTTPSource(url, user, pwd string) (Source, error) {
	return newBasicHTTPSource(url, user, pwd)
}

// NewGetterSource takes
func NewGetterSource(get SourceGetter) (Source, error) {
	return newGetterSource(get, io.Copy)
}

// NewSource returns a NewSource component using a getter function as its input stream
func NewSource(adapter SourceAdapter) (Source, error) {
	return newSource(adapter, io.Pipe)
}

func newSource(
	adapter SourceAdapter,
	ioPipe func() (*io.PipeReader, *io.PipeWriter),
) (Source, error) {
	if adapter == nil {
		return nil, errors.New("adapter is nil")
	}
	if ioPipe == nil {
		return nil, errors.New("ioPipe is nil")
	}
	return func(f Feedback) (io.ReadCloser, error) {
		if f == nil {
			return nil, errors.New("feedback funcion is nil")
		}
		pr, pw := ioPipe()
		go func() {
			defer pw.Close()
			adapter(f, pw)
		}()
		return pr, nil
	}, nil
}

func newGetterSource(
	get SourceGetter,
	iocopy func(io.Writer, io.Reader) (int64, error),
) (Source, error) {
	if get == nil {
		return nil, errors.New("get function is nil")
	}
	if iocopy == nil {
		return nil, errors.New("iocopy function is nil")
	}
	adapter := func(f Feedback, w io.Writer) {
		r, err := get()
		if err != nil {
			f("error on retrieving reader %v", err)
			return
		}
		if _, err := iocopy(w, r); err != nil {
			f("error on writing %v", err)
		}
	}
	return NewSource(adapter)
}

func newBasicHTTPSource(url, user, pwd string) (Source, error) {
	get := func() (io.ReadCloser, error) {
		client := &http.Client{}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(user, pwd)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			body, _ := ioutil.ReadAll(resp.Body) // consumes all body before leaves
			return nil, errors.New(resp.Status + ":" + string(body))
		}
		return resp.Body, nil
	}
	return NewGetterSource(get)
}
