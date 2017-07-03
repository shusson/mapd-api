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
	"regexp"
	"fmt"
	"flag"
	"os"
	"strings"
	"time"
	"syscall"
	"os/signal"
	"github.com/gorilla/mux"
)

type opts struct {
	url string
	user string
	db string
	pwd string
	httpPort int
	bufferSize int
}

type MapDCon struct {
	client *mapd.MapDClient
	session mapd.TSessionId
	options opts
}

func main() {

	options := options()

	con, err := retry(60, 2 * time.Second, func() (*MapDCon, error) {
		log.Println("connecting to mapd server...")
		return connectToMapD(options)
	})
	if err != nil || con.session == "" {
		log.Fatal("failed to connect to mapd server")
	}
	log.Println("connected to mapd server: ", con.session)
	defer con.client.Disconnect(con.session)
	defer con.client.Transport.Close()

	handleSystemSignals(con)

	r := mux.NewRouter()
	r.HandleFunc("/healthcheck", healthCheck(con))
	r.HandleFunc("/", sessionProxy(con))
	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", options.httpPort), r))
}

func sessionProxy(con *MapDCon) http.HandlerFunc {
	serverUrl, err := url.Parse(con.options.url)
	if err != nil {
		log.Fatal("URL failed to parse")
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(serverUrl)
	modified := modifySession(reverseProxy, con)

	return http.HandlerFunc(modified)
}

func modifySession(handler http.Handler, con *MapDCon) http.HandlerFunc {
	nonce := 0
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err == nil {
			nonce++
			b := string(body[:])
			if strings.Contains(b, "sql_execute") {
				re := regexp.MustCompile(`(\[\d,")(\w*)(",\d,\d,{"1":{"str":")(\w*)(".*},"2".*,"3".*,"4":{"str":")(\d*)("}.*,"5".*}\])`)
				repl := fmt.Sprintf("${1}${2}${3}%s${5}%d${7}", con.session, nonce)
				b = re.ReplaceAllString(b, repl)
				body = []byte(b)
				// when writing a request the http lib ignores the request header and reads from the ContentLength field
				// http://tip.golang.org/pkg/net/http/#Request.Write
				// https://github.com/golang/go/issues/7682
				r.ContentLength = int64(len(b))
			} else if strings.Contains(b, "get_table_details") {
				re := regexp.MustCompile(`(.*{"str":")(\w{32})(.*)`)
				repl := fmt.Sprintf("${1}%s${3}", con.session)
				b = re.ReplaceAllString(b, repl)
				body = []byte(b)
				r.ContentLength = int64(len(b))
			}
		}
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		handler.ServeHTTP(w, r)
	}
}

func connectToMapD(options opts) (*MapDCon, error) {
	protocolFactory := thrift.NewTJSONProtocolFactory()
	transportFactory := thrift.NewTBufferedTransportFactory(options.bufferSize)
	socket, err := thrift.NewTHttpPostClient(options.url)
	if err != nil {
		return nil, err
	}
	transport, err := transportFactory.GetTransport(socket)
	if err != nil {
		return nil, err
	}
	if err := transport.Open(); err != nil {
		return nil, err
	}
	client := mapd.NewMapDClientFactory(transport, protocolFactory)
	sessionId, err := client.Connect(options.user, options.pwd, options.db)
	if err != nil {
		return nil, err
	}

	return &MapDCon{client, sessionId, options}, err
}

func healthCheck(con *MapDCon) http.HandlerFunc {
	fn := func(w http.ResponseWriter, r *http.Request) {
		log.Println("Performing Healthcheck...")
		if con.session == "" {
			em := "Missing SessionId"
			http.Error(w, em, 500)
			log.Println("Healthcheck failed: " + em)
		} else {
			tbs, err := con.client.GetTables(con.session)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			con.client.GetTables(con.session)
			for i := 0; i < len(tbs); i++ {
				res, err := con.client.SqlExecute(con.session, "SELECT COUNT(*) FROM " + tbs[i], true, "0", 1)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				numRows := len(res.RowSet.Columns[0].Nulls)
				numCols := len(res.RowSet.RowDesc)
				for r := 0; r < numRows; r++ {
					for c := 0; c < numCols; c++ {
						fmt.Fprintf(w, "%s: %d\n", tbs[i], res.RowSet.Columns[c].Data.IntCol[r])
					}
				}
			}
			fmt.Fprintln(w, "OK")
		}
	}
	return http.HandlerFunc(fn)
}

func handleSystemSignals(con *MapDCon) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c
		log.Println("Terminating due to signal: ", sig.String())
		con.client.Disconnect(con.session)
		con.client.Transport.Close()
		os.Exit(1)
	}()
}

func retry(attempts int, sleep time.Duration, action func() (*MapDCon, error)) (*MapDCon, error) {
	var err error
	var con *MapDCon
	for i := 0; i < attempts; i++ {
		con, err = action()
		if err == nil {
			return con, err
		}
		time.Sleep(sleep)
		log.Println("retrying action after error: ", err)
	}
	return con, err
}

func options() opts {
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
	return opts{mapdUrl, mapdUser, mapdDb, mapdPwd, httpPort, bufferSize}
}