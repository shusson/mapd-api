package mapdutil

import (
	"github.com/shusson/mapd-api/thrift/e745367/mapd"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"encoding/json"
	"time"
	"sync"
)

// MapDConn wrapper around mapd client
type MapDConn struct {
	Client  *mapd.MapDClient
	Session mapd.TSessionId
	Mu sync.Mutex
}

// MapDConnInfo mapd connection info
type MapDConnInfo struct {
	Version   string `json:"version"`
	StartTime int64 `json:"start_time"`
	ReadOnly  bool `json:"read_only"`
}

// ConnectToMapD connect to mapd core server
func ConnectToMapD(user string, pwd string, db string, url string, bufferSize int) (*MapDConn, error) {
	protocolFactory := thrift.NewTJSONProtocolFactory()
	transportFactory := thrift.NewTBufferedTransportFactory(bufferSize)
	socket, err := thrift.NewTHttpPostClient(url)
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
	sessionID, err := client.Connect(user, pwd, db)
	if err != nil {
		return nil, err
	}

	log.Println("connected to mapd server: ", sessionID)
	conn := &MapDConn{Client: client, Session: sessionID}
	ConnectionInfo(conn)
	info, err := ConnectionInfo(conn)
	if err != nil {
		return nil, err
	}
	jInfo, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	log.Println(string(jInfo))

	return conn, err
}

// ConnectToMapDWithRetry connect to mapd core server with a retry
func ConnectToMapDWithRetry(user string, pwd string, db string, url string, bufferSize int, attempts int, sleep time.Duration) (*MapDConn, error) {
	return retry(attempts, sleep, func() (*MapDConn, error) {
		log.Println("connecting to mapd server...")
		return ConnectToMapD(user, pwd, db, url, bufferSize)
	})
}

// ConnectionInfo get mapd connection info
func ConnectionInfo(con *MapDConn) (*MapDConnInfo, error) {
	serverInfo, err := con.Client.GetServerStatus(con.Session)
	if err != nil {
		return nil, err
	}
	hcr := &MapDConnInfo{ReadOnly: serverInfo.ReadOnly, StartTime: serverInfo.StartTime, Version: serverInfo.Version}
	return hcr, nil
}

func retry(attempts int, sleep time.Duration, action func() (*MapDConn, error)) (*MapDConn, error) {
	var err error
	var con *MapDConn
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