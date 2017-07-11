package main

import (
	"net/url"
	"log"
	"net/http"
	"io/ioutil"
	"regexp"
	"fmt"
	"flag"
	"os"
	"strings"
	"time"
	"syscall"
	"os/signal"
	"github.com/gorilla/mux"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/shusson/mapd-api/redisutil"
	"github.com/shusson/mapd-api/proxyutil"
	"errors"
	"github.com/shusson/mapd-api/mapdutil"
)

type opts struct {
	url        *url.URL
	user       string
	db         string
	pwd        string
	httpPort   int
	bufferSize int
	redisURL   string
}

func main() {

	options, err := options()
	if err != nil  {
		log.Fatal("failed parse flag options: " + err.Error())
	}

	cache := redisutil.NewPool(options.redisURL)
	defer cache.Close()

	conn, err := mapdutil.ConnectToMapDWithRetry(options.user, options.pwd, options.db, options.url.String(), options.bufferSize, 60, 2*time.Second)
	if err != nil || conn.Session == "" {
		log.Fatal("failed to connect to mapd server")
	}

	defer conn.Client.Disconnect(conn.Session)
	defer conn.Client.Transport.Close()

	sigHandler(conn, cache)

	r := mux.NewRouter()
	r.HandleFunc("/healthcheck", healthCheck(conn))
	r.HandleFunc("/", handleThriftRequests(conn, cache, options))
	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", options.httpPort), r))
}

func handleThriftRequests(conn *mapdutil.MapDCon, cache *redis.Pool, options opts) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		b := string(body[:])

		if strings.Contains(b, "sql_execute") {
			query, err := getSQLQuery(b)
			if err != nil {
				http.Error(w, err.Error(), 502)
			}

			result, err := redisutil.Get(cache, query)
			if err != nil {
				replaceSessionString(b, conn)
				t := &proxyutil.Transport{RoundTripper: http.DefaultTransport, Key: query, Pool: cache}
				proxyutil.ReverseProxy(w, r, []byte(b), options.url, t)
			} else {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Content-Type", "application/x-thrift")
				fmt.Fprintln(w, string(result))
			}
		} else if strings.Contains(b, "get_table_details") {
			replaceSessionString(b, conn)
			t := &proxyutil.Transport{RoundTripper: http.DefaultTransport, Key: "", Pool: cache}
			proxyutil.ReverseProxy(w, r, []byte(b), options.url, t)
		} else {
			t := &proxyutil.Transport{RoundTripper: http.DefaultTransport, Key: "", Pool: cache}
			proxyutil.ReverseProxy(w, r, []byte(b), options.url, t)
		}
	}
}

func getSQLQuery(s string) (string, error){
	re := regexp.MustCompile(`(.*,"2":{"str":")(.*)("},"3".*)`)
	m := re.FindStringSubmatch(s)
	if m == nil || len(m) != 4 {
		return "", errors.New("Could not find SQL query in body of request")
	}
	return m[2], nil
}

func replaceSessionString(s string, conn *mapdutil.MapDCon) string {
	re := regexp.MustCompile(`(.*{"str":")(\w{32})(.*)`)
	repl := fmt.Sprintf("${1}%s${3}", conn.Session)
	return re.ReplaceAllString(s, repl)
}

func healthCheck(con *mapdutil.MapDCon) http.HandlerFunc {
	handleError := func(w http.ResponseWriter, err error) error {
		if err != nil {
			http.Error(w, err.Error(), 500)
			log.Println("Healthcheck failed: " + err.Error())
		}
		return err
	}

	fn := func(w http.ResponseWriter, r *http.Request) {

		info, err := mapdutil.ConnectionInfo(con)
		if handleError(w, err) != nil {
			return
		}
		jInfo, err := json.Marshal(info)
		if handleError(w, err) != nil {
			return
		}
		fmt.Fprintln(w, string(jInfo))
		log.Println("Healthcheck passed - " + string(jInfo))
	}
	return http.HandlerFunc(fn)
}

func sigHandler(con *mapdutil.MapDCon, cache *redis.Pool) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c
		log.Println("Terminating due to signal: ", sig.String())
		con.Client.Disconnect(con.Session)
		con.Client.Transport.Close()
		cache.Close()
		os.Exit(1)
	}()
}

func options() (opts, error) {
	var mapdURL string
	var mapdUser string
	var mapdDb string
	var mapdPwd string
	var httpPort int
	var bufferSize int
	var cacheURL string
	flag.StringVar(&mapdURL, "url", "http://127.0.0.1:80", "url to mapd-core server")
	flag.StringVar(&mapdUser, "user", "mapd", "mapd user")
	flag.StringVar(&mapdDb, "db", "mapd", "mapd database")
	flag.StringVar(&mapdPwd, "pass", "HyperInteractive", "mapd pwd")
	flag.IntVar(&httpPort, "http-port", 4000, "port to listen to incoming http connections")
	flag.IntVar(&bufferSize, "b", 8192, "thrift transport buffer size")
	flag.StringVar(&cacheURL, "redisURL", "localhost:6379", "URL to redis, if empty no cache is used")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	serverURL, err := url.Parse(mapdURL)
	if err != nil {
		return opts{}, err
	}
	return opts{serverURL, mapdUser, mapdDb, mapdPwd, httpPort, bufferSize, cacheURL}, nil
}
