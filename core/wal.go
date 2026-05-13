package core

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type Waler interface {
	Publish(msg proto.Message) error
	Replay() (*map[Key]Value, error)
	flush() error
}

type Wal struct {
	file *os.File
	path string
	mu   sync.Mutex
}

func newWal(path string) (*Wal, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{file, path, sync.Mutex{}}, nil
}

func (w *Wal) Publish(msg proto.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := w.file.Write(header); err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *Wal) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Close()
}

func (w *Wal) Replay() (*map[Key]Value, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	results := make(map[Key]Value)

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(w.file)
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		size := binary.BigEndian.Uint32(header)
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}

		var entry pb.WalEntry
		if err := proto.Unmarshal(payload, &entry); err != nil {
			return nil, err
		}

		switch kv := entry.Entry.(type) {
		case *pb.WalEntry_Set:
			results[kv.Set.Key] = kv.Set.Value
		case *pb.WalEntry_Delete:
			delete(results, kv.Delete.Key)
		}
	}
	return &results, nil
}

func (w *Wal) flush() error {
	return w.file.Truncate(0)
}
