package main

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shusson/mapd-api/thrift/e745367/mapd"
	"net/url"
	"log"
	"net/http/httputil"
	"net/http"
	"io/ioutil"
	"bytes"
	"errors"
	"regexp"
	"fmt"
	"flag"
	"os"
)

type mapdOptions struct {
	Url string
	User string
	Db string
	Pwd string
}

func main() {

	var mapdUrl string
	var mapdUser string
	var mapdDb string
	var mapdPwd string
	var httpPort int
	var bufferSize int
	flag.StringVar(&mapdUrl, "url", "http://127.0.0.1:80", "url to mapd-core server")
	flag.StringVar(&mapdUser, "user", "mapd", "mapd user")
	flag.StringVar(&mapdDb, "db", "mapd", "mapd database")
	flag.StringVar(&mapdPwd, "pass", "HyperInteractive", "mapd pwd")
	flag.IntVar(&httpPort, "http-port", 4000, "port to listen to incoming http connections")
	flag.IntVar(&bufferSize, "b", 8192, "thrift transport buffer size")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	protocolFactory := thrift.NewTJSONProtocolFactory()
	transportFactory := thrift.NewTBufferedTransportFactory(bufferSize)

	mapdClient, sessionId, err := connectToMapd(transportFactory, protocolFactory, mapdOptions{mapdUrl, mapdUser, mapdDb, mapdPwd})
	if err != nil || sessionId == "" {
		log.Fatal("failed to get sessionId")
	}
	defer mapdClient.Disconnect(sessionId)

	proxy := sessionProxy(mapdUrl, string(sessionId))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", httpPort), proxy))
}

func sessionProxy(remoteUrl string, sessionId string) http.Handler {
	serverUrl, err := url.Parse(remoteUrl)
	if err != nil {
		log.Fatal("URL failed to parse")
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(serverUrl)
	modified := modifySession(reverseProxy, sessionId)
	return modified
}

func modifySession(handler http.Handler, sessionId string) http.Handler {
	nonce := 0
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err == nil {
			nonce++
			b := string(body[:])
			re := regexp.MustCompile(`(\[\d,")(\w*)"(,\d,\d,{"1":{"str":")(\w*)(".*},"2".*,"3".*,"4":{"str":")(\d*)("}.*,"5".*}\])`)
			repl := fmt.Sprintf("${1}${2}${3}%s${5}%d${7}", sessionId, nonce)
			b = re.ReplaceAllString(b, repl)
		}
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		handler.ServeHTTP(w, r)
	})
}

func connectToMapd(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, options mapdOptions) (*mapd.MapDClient, mapd.TSessionId, error) {
	socket, err := thrift.NewTHttpPostClient(options.Url)
	if socket == nil {
		return nil, "", errors.New("nil transport")
	}
	if err != nil {
		return nil, "", err
	}
	transport, err := transportFactory.GetTransport(socket)
	if err != nil {
		return nil, "", err
	}
	if transport == nil {
		return nil, "", errors.New("nil transport")
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return nil, "", err
	}
	client := mapd.NewMapDClientFactory(transport, protocolFactory)
	sessionId, err := client.Connect(options.User, options.Pwd, options.Db)
	if err != nil {
		return nil, "", err
	}
	return client, sessionId, err
}