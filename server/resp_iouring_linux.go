//go:build linux

package server

import (
	"github.com/rosewrightdev/oryx"
)

type ioUringServer struct {
	server *RESPServer
}

func newIOUringServer(eng oryx.Database, addr string) *ioUringServer {
	return &ioUringServer{
		server: NewRESPServer(eng, addr),
	}
}

// RunIOUring executes the Linux RESP reactor engine.
func (s *RESPServer) RunIOUring() error {
	return s.Run()
}

func (s *ioUringServer) Addr() string {
	return s.server.Addr()
}

func (s *ioUringServer) Run() error {
	return s.server.Run()
}

func (s *ioUringServer) Stop() {
	s.server.Stop()
}
