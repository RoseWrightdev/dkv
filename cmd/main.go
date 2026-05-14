package main

import (
	"net"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/server"
)

func main() {
	eb := dkv.NewEngineBuilder()
	eb.SetSssInterval(time.Duration(3) * time.Minute)
	eb.SetWalSyncInterval(time.Duration(500) * time.Microsecond)
	eb.SetSssPath("sss.json")
	eb.SetWalPath("wal.binpb")

	eng, err := eb.GetEngine()
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
