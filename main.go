package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"bytes"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/shusson/mapd-api/mapd"
)

func main() {

	protocolFactory := thrift.NewTSimpleJSONProtocolFactory()
	transportFactory := thrift.NewTBufferedTransportFactory(8192)

	//if *framed {
	//	transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	//}

	if err := runClient(transportFactory, protocolFactory, "127.0.0.1:80", false); err != nil {
		fmt.Println("error running client:", err)
	}
}

func Index(router *mux.Router, url string) http.HandlerFunc {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var jsonStr = []byte(`[1,"connect",1,0,{"1":{"str":"mapd"},"2":{"str":"HyperInteractive"},"3":{"str":"mapd"}}]`)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))

		if check(err, w) != nil {
			return
		}
		req.Header.Set("Accept", "application/vnd.apache.thrift.json; charset=utf-8")
		req.Header.Set("Content-Type", "application/vnd.apache.thrift.json; charset=UTF-8")
		req.Header.Set("Content-Length", "88")

		client := &http.Client{}
		resp, err := client.Do(req)
		if check(err, w) != nil {
			return
		}
		defer resp.Body.Close()

		var status error
		if resp.StatusCode != 200 {
			status = errors.New(resp.Status)
		}
		if check(status, w) != nil {
			return
		}
		w.Header().Set("Server", "Mapd healthcheck")
		w.WriteHeader(200)
		w.Write([]byte("OK"))
	}
	return http.HandlerFunc(fn)
}

func check(err error, w http.ResponseWriter) error {
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	return err
}


func handleClient(client *mapd.MapDClient) (err error) {
	sessionId, err := client.Connect("mapd", "HyperInteractive", "mapd")
	println(sessionId)
	if err != nil {
		fmt.Println("Unable to connect:", err)
		return err
	}
	return err
}

func runClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	//var transport thrift.TTransport
	//var err error
	//if secure {
	//	cfg := new(tls.Config)
	//	cfg.InsecureSkipVerify = true
	//	transport, err = thrift.NewTSSLSocket(addr, cfg)
	//} else {
	//	transport, err = thrift.NewTSocket(addr)
	//}
	var transport thrift.TTransport
	//noinspection ALL
	transport, err := thrift.NewTSocket(addr)
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	if transport == nil {
		return fmt.Errorf("Error opening socket, got nil transport. Is server available?")
	}
	transport, err = transportFactory.GetTransport(transport)
	if transport == nil {
		return fmt.Errorf("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return err
	}

	return handleClient(mapd.NewMapDClientFactory(transport, protocolFactory))
}