// simple dynamo database
// 32 partitions
// n=1 w=1 r=1

package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DEFAULT_SERVER = "localhost"
const DEFAULT_PORT = 4000
const DEFAULT_DB = "db"

var GET = []byte("get ")
var PUT = []byte("put ")
var DEL = []byte("del ")
var CON = []byte("con ")

var SPACE = []byte(" ")[0]
var NEXT = []byte("\n")[0]

var myServer string

// leveldb is not thread safe
var dbLock *sync.RWMutex

// represents server connections
// slice of server names
// will always be sorted

// also not thread safe
var serverListLock *sync.RWMutex
var serverConns map[string]bool
var sortedServers []string

func main() {

	thisServerPtr := flag.String("server", DEFAULT_SERVER, "servername or ip address")
	thisPortPtr := flag.Int("port", DEFAULT_PORT, "port to take commands on")
	thisDBPtr := flag.String("db", DEFAULT_DB, "database folder")

	flag.Parse()

	thisServer := *thisServerPtr
	thisPort := *thisPortPtr
	thisDB := *thisDBPtr

	myServer = thisServer + ":" + strconv.Itoa(thisPort)

	dbLock = new(sync.RWMutex)

	serverListLock = new(sync.RWMutex)
	serverConns = make(map[string]bool)
	serverConns[myServer] = true
	sortedServers = []string{myServer}

	log("start server")
	db := startDB(thisDB)

	// recieve net connections (net.Conn), send to tcp
	conns := handleConns(myServer)

	log("run on " + myServer + "\n")

	// tcp, send to commandChan or relayChan
	handleTcp(conns, db)

	connectAll()

	// run forever
	block := make(chan int)
	<-block
}

// periodically make sure all nodes are properly connected
func connectAll() {

	go func() {

		for {
			// 10 seconds
			time.Sleep(10000 * time.Millisecond)

			curServerList := getServerList()

			log("server list")
			allStr := ""
			for _, server := range curServerList {
				allStr += server + " "
			}
			log(allStr)

			for _, serverStr := range curServerList {

				// ignore myServer
				if serverStr != myServer {

					// send out con command for self connect
					conn, err := net.Dial("tcp", serverStr)
					if err != nil {
						fmt.Printf("connect couldn't dial: " + err.Error() + "\n")
						continue
					}

					sendThis := []byte("con " + myServer + "\n")
					conn.Write(sendThis)

					reader := bufio.NewReader(conn)

					data, err := reader.ReadString('\n')
					conn.Close()

					// receive and confirm
					if err != nil {
						fmt.Println("connect error " + err.Error())
						continue
					}

					// add possibly missing data
					data = strings.TrimSpace(data)
					toAdd := strings.Split(data, " ")
					makeServerListGroup(toAdd)
				}
			}
		}
	}()
}

func logTrim(thisStr string) {
	fmt.Printf(thisStr)
}

func log(thisStr string) {
	fmt.Println(thisStr)
}

// uses leveldb
func startDB(dbFolder string) *levigo.DB {

	// 16 megabytes
	size := 1 << 24

	log("create database in file " + dbFolder + " of size " + strconv.Itoa(size) + " bytes")

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(size))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(dbFolder, opts)

	if err != nil {
		log("couldn't start listening: " + err.Error() + "\n")
		os.Exit(0)
	}

	return db
}

func handleConns(serverName string) chan net.Conn {

	sendTo := serverName

	listener, err := net.Listen("tcp", sendTo)

	if err != nil {
		log("couldn't start listening: " + err.Error())
		os.Exit(0)
	}

	ch := make(chan net.Conn)

	go func() {

		for {
			client, err := listener.Accept()

			if err != nil {
				log("couldn't accept: " + err.Error())
				continue
			}
			ch <- client
		}
	}()
	return ch
}

// may need to relay command to other server
func potentialRelay(line []byte) ([]byte, string, error) {

	keyInd := bytes.IndexByte(line[4:], SPACE)

	var key []byte
	if keyInd == -1 {
		key = line[4 : len(line)-1]
	} else {
		key = line[4 : keyInd+4]
	}

	// 32 partitions divided between servers
	// hash of key goes to one partition

	h := sha1.New()
	io.WriteString(h, string(key))
	section := h.Sum(nil)[0] % 32

	curServerList := getServerList()

	serverInd := int(section) % len(curServerList)
	relayServer := curServerList[serverInd]

	// if it is a relay, net out
	if relayServer != myServer {

		conn, err := net.Dial("tcp", relayServer)
		if err != nil {
			fmt.Printf("couldn't dial: " + err.Error() + "\n")
			return nil, "", err
		}

		sendThis := line
		conn.Write(sendThis)

		reader := bufio.NewReader(conn)

		data, err := reader.ReadString('\n')
		conn.Close()

		if err != nil {
			fmt.Println("error " + err.Error())
			return nil, "", err
		}

		return []byte(data), relayServer, err
	}

	return nil, "", nil
}

// create copy
func getServerList() []string {

	serverListLock.RLock()

	newList := make([]string, len(sortedServers))
	copy(newList, sortedServers)

	serverListLock.RUnlock()

	return newList
}

func makeServerListGroup(serverNames []string) {
	serverListLock.Lock()

	for _, serverName := range serverNames {
		serverConns[serverName] = true
	}

	sortedServers = make([]string, len(serverConns))
	i := 0
	for key, _ := range serverConns {
		sortedServers[i] = key
		i++
	}
	sort.Strings(sortedServers)

	serverListLock.Unlock()
}

func makeServerList(serverName string) {
	all := []string{serverName}
	makeServerListGroup(all)
}

// client and server use same serialization
// this uses an accept tcp channel and tcp data reading channel
func handleTcp(conns chan net.Conn, db *levigo.DB) {

	go func() {

		for {

			client := <-conns

			go func() {
				b := bufio.NewReader(client)

				for {
					line, err := b.ReadBytes('\n')
					if err != nil {
						break
					}

					// for data error
					err = nil
					var data []byte

					log("recv on: " + myServer)

					// determine if need to relay
					if bytes.Equal(PUT, line[0:4]) ||
						bytes.Equal(GET, line[0:4]) ||
						bytes.Equal(DEL, line[0:4]) {

						// returns nil, nil, nil if there is no relay
						data, relayName, relayErr := potentialRelay(line)

						// return the result
						if data != nil {

							log("recv: " + string(line[:len(line)-1]))

							client.Write(data)
							log("relay from " + relayName + ": " + string(data))
							continue
						}

						if relayErr != nil {
							log("relay error: " + relayErr.Error())
							continue
						}
					}

					sendBack := []byte("failed\n")

					if bytes.Equal(PUT, line[0:4]) {

						keyInd := bytes.IndexByte(line[4:], SPACE) + 4
						key := line[4:keyInd]
						val := line[keyInd+1 : len(line)-1]

						log("recv: put " + string(key) + " " + string(val))

						wo := levigo.NewWriteOptions()

						dbLock.Lock()
						err = db.Put(wo, key, val)
						dbLock.Unlock()

						sendBack = []byte("success\n")

					} else if bytes.Equal(GET, line[0:4]) {

						key := line[4 : len(line)-1]

						log("recv: get " + string(key))

						ro := levigo.NewReadOptions()

						dbLock.RLock()
						data, err = db.Get(ro, key)
						dbLock.RUnlock()

						// when fail to get, send back empty string
						sendBack = append(data, NEXT)

					} else if bytes.Equal(DEL, line[0:4]) {

						key := line[4 : len(line)-1]

						log("recv: del " + string(key))

						wo := levigo.NewWriteOptions()

						dbLock.Lock()
						err = db.Delete(wo, key)
						dbLock.Unlock()

						sendBack = []byte("success\n")

					} else if bytes.Equal(CON, line[0:4]) {

						// acknowledge to other servers

						serverName := string(line[4 : len(line)-1])
						log("recv: connect " + serverName)

						makeServerList(serverName)
						curServerList := getServerList()

						// must make sure all servers are shared
						sendStr := ""
						for _, val := range curServerList {
							sendStr += val + " "
						}

						sendStr += "\n"
						sendBack = []byte(sendStr)

					} else {
						log("recv: unrecognized command")
						sendBack = []byte("unrecognized\n")
					}

					if err != nil {
						thisError := "database error: " + err.Error() + "\n"
						sendBack = []byte(thisError)
					}

					client.Write(sendBack)
					log("send: " + string(sendBack))
				}

				log("end connection\n")
			}()
		}
	}()
}
