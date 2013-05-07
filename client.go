// simple client test

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func logTrim(thisStr string) {
	fmt.Printf(thisStr)
}

func sendRecv(conn net.Conn, sendStr string) {

	logTrim("send: " + sendStr)

	sendThis := []byte(sendStr)
	conn.Write(sendThis)

	reader := bufio.NewReader(conn)

	stats, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("error " + err.Error())
	}

	logTrim("recv: " + stats)
}

func main() {
	fmt.Println("start")

	// 4000 is the client we connect to
	conn, err := net.Dial("tcp", "localhost:4000")
	if err != nil {
		fmt.Printf("couldn't dial: " + err.Error() + "\n")
		os.Exit(0)
	}

	sendRecv(conn, "con localhost:4002\n")

	sendRecv(conn, "con localhost:4001\n")

	sendRecv(conn, "put thiskey thisval\n")

	sendRecv(conn, "get thiskey\n")

	sendRecv(conn, "del thiskey\n")

	sendRecv(conn, "get thiskey\n")

	sendRecv(conn, "put otherkey otherval\n")

	sendRecv(conn, "put blahkey blahval\n")

	sendRecv(conn, "put checkkey checkval\n")

	sendRecv(conn, "put emptykey emptyval\n")

	conn.Close()
}
