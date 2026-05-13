package main

import (
	"net"
	"time"

	"github.com/rosewrightdev/dkv/core"
	"github.com/rosewrightdev/dkv/server"
)

func main() {
	eb := core.NewEngineBuilder().SetSssInterval(time.Duration(3) * time.Minute).SetSssPath().SetWalPath()
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
