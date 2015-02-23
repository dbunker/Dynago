Dynago
======

To test out the simplified Dynamo database run each of the queries below in terminal.
This will run the nodes on ports 4000, 4001, and 4002 with each node's associated LevelDB database stored in folders named
4000, 4001, and 4002 respectively. More information on LevelDB can be found [here](https://github.com/google/leveldb).

	go run server.go -port 4000 -db 4000

	go run server.go -port 4001 -db 4001

	go run server.go -port 4002 -db 4002

Once we have some nodes running, we can run the test client to link them together and store, retrieve and delete data.

	go run client.go

This project is a work in progress and will be subject to change.
