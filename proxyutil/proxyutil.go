package proxyutil

import (
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
	"github.com/shusson/mapd-api/redisutil"
	"bytes"
	"net/http"
	"net/url"
	"net/http/httputil"
)

// Transport we implement our own transport layer so that we can intercept the response in the reverse proxy
type Transport struct {
	http.RoundTripper
	Key string
	Pool *redis.Pool
}

// RoundTrip intercept the response from mapd and cache the value in redis
func (t *Transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	resp, err = t.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if t.Key != "" {
		redisutil.Set(t.Pool, t.Key, b)
	}

	body := ioutil.NopCloser(bytes.NewReader(b))
	resp.Body = body
	resp.ContentLength = int64(len(b))
	return resp, nil
}

// ReverseProxy reverse proxies to a server
func ReverseProxy(w http.ResponseWriter, r *http.Request, body []byte, serverURL *url.URL, t *Transport) {
	// when writing a request the http lib ignores the request header and reads from the ContentLength field
	// http://tip.golang.org/pkg/net/http/#Request.Write
	// https://github.com/golang/go/issues/7682
	r.ContentLength = int64(len(body))
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	handler := httputil.NewSingleHostReverseProxy(serverURL)
	if t != nil {
		handler.Transport = t
	}
	handler.ServeHTTP(w, r)
}
