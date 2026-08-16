//go:build !linux

package server

type ioUringServer struct{}

func (s *ioUringServer) Addr() string { return "" }
func (s *ioUringServer) Stop()        {}

func (s *RESPServer) RunIOUring() error {
	return s.Run()
}
