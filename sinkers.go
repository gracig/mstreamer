package mstreamer

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
)

// SinkerAdapter takes
type SinkerAdapter func(Feedback, io.ReadCloser) error

// PushSinker takes
type PushSinker func(Feedback, io.ReadCloser) error

// NewBasicHTTPSinker is
func NewBasicHTTPSinker(url, user, pwd string) (Sinker, error) {
	return newBasicHTTPSinker(url, user, pwd)
}

// NewStdoutSinker is
func NewStdoutSinker() (Sinker, error) {
	return NewWriterSinker(os.Stdout)
}

// NewTCPSinker takes an address
func NewTCPSinker(addr string) (Sinker, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewWriteCloserSinker(conn)
}

// NewWriterSinker takes
func NewWriterSinker(out io.Writer) (Sinker, error) {
	push := func(f Feedback, r io.ReadCloser) error {
		if _, err := io.Copy(out, r); err != nil {
			return err
		}
		return nil
	}
	return NewPushSinker(push)
}

// NewWriteCloserSinker takes
func NewWriteCloserSinker(out io.WriteCloser) (Sinker, error) {
	push := func(f Feedback, r io.ReadCloser) error {
		defer out.Close()
		if _, err := io.Copy(out, r); err != nil {
			return err
		}
		return nil
	}
	return NewPushSinker(push)
}

// NewPushSinker takes
func NewPushSinker(push PushSinker) (Sinker, error) {
	return newPushSinker(push)
}

// NewSinker takes
func NewSinker(adapter SinkerAdapter) (Sinker, error) {
	return newSinker(adapter)
}

func newSinker(
	adapter SinkerAdapter,
) (Sinker, error) {
	if adapter == nil {
		return nil, errors.New("adapter is nil")
	}
	return func(f Feedback, r io.ReadCloser) error {
		if f == nil {
			return errors.New("feedback funcion is nil")
		}
		if r == nil {
			return errors.New("reader stream is nil")
		}
		return adapter(f, r)
	}, nil
}

func newPushSinker(push PushSinker) (Sinker, error) {
	if push == nil {
		return nil, errors.New("push function is nil")
	}
	adapter := func(f Feedback, r io.ReadCloser) error {
		return push(f, r)
	}
	return NewSinker(adapter)
}

func newBasicHTTPSinker(url, user, pwd string) (Sinker, error) {
	push := func(f Feedback, r io.ReadCloser) error {
		client := &http.Client{}
		req, err := http.NewRequest("POST", url, r)
		if err != nil {
			f("error %v", err)
			return err
		}
		req.SetBasicAuth(user, pwd)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
			body, _ := ioutil.ReadAll(resp.Body) // consumes all body before leaves
			return fmt.Errorf("error %v, status: %v, body: %v ", err, resp.Status, string(body))
		}
		return nil
	}
	return NewPushSinker(push)
}
