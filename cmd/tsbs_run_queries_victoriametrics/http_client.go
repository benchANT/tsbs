package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/benchant/tsbs/pkg/query"
)

var bytesSlash = []byte("/") // heap optimization

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	//client     fasthttp.Client
	client     *http.Client
	// Host       []byte
	// HostString string
	// uri        []byte
	Url				string
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	// Debug                int
	PrettyPrintResponses bool
	// chunkSize            uint64
	// database             string
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 1024,
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(url string) *HTTPClient {
	/*if strings.HasSuffix(host, "/") {
		host = host[:len(host)-1]
	}*/
	return &HTTPClient{
		client:     getHttpClient(),
	//	Host:       []byte(host),
	//	HostString: host,
	//	uri:        []byte{}, // heap optimization
		Url:		url
	}
}


func (p *processor) do(q *query.HTTP) (float64, error) {





}


// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), w.Url+string(q.Path), nil)
	if err != nil {
		return 0, fmt.Errorf("error while creating request: %s", err)
	}
	/* if err != nil { panic(err) } */

	start := time.Now()
	// resp, err := http.DefaultClient.Do(req)
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("query execution error: %s", err)
	}
	/* if err != nil { panic(err) } */
	defer resp.Body.Close()

	/*if resp.StatusCode != http.StatusOK {
		panic("http request did not return status 200 OK")
	}*/
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("non-200 statuscode received: %d; Body: %s", resp.StatusCode, string(body))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error while reading response body: %s", err)
	}
	// if err != nil { panic(err) }
	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	// Pretty print JSON responses, if applicable:
	if opts != nil && opts.PrettyPrintResponses {
		var pretty bytes.Buffer
		prefix := fmt.Sprintf("ID %d: ", q.GetID())
		if err := json.Indent(&pretty, body, prefix, "  "); err != nil {
			return lag, err
		}
		_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, pretty.Bytes())
		if err != nil {
			return lag, err
		}
	}
	return lag, nil
}