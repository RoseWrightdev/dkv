package main

import (
	"net"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/server"
)

func main() {
	eng, err := dkv.NewEngineBuilder().
		SetSssInterval(time.Duration(3) * time.Minute).
		SetWalSyncInterval(time.Duration(500) * time.Microsecond).
		SetSssPath("sss.json").
		SetWalPath("wal.binpb").
		SetWalBufferSize(64 * 1028).
		GetEngine()
	if err != nil {
		panic(err)
	}

	s := server.NewGrpc(eng)
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	err = s.Run(listener)
	if err != nil {
		panic(err)
	}
	defer s.Stop()
}
