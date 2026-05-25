package dkv

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/credentials/insecure"
)

func BenchmarkNewCluster(b *testing.B) {
	for _, nodes := range []int{100, 200, 400, 800, 1600} {
		b.Run(fmt.Sprintf("%d", nodes), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cluster, err := NewCluster(nodes, "", insecure.NewCredentials())
				if err != nil {
					b.Fatal(err)
				}
				cluster.Stop()
			}
		})
	}
}
