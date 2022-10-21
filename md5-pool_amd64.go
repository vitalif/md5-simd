package md5simd

import (
	"fmt"
	"sync"
)

type HashPool interface {
	Get() Hasher
	Put(client Hasher) error
	Close()
}

type md5PoolServer struct {
	mu          sync.Mutex
	options     ServerOptions
	servers     []Server
	freeClients [][]*md5PoolDigest
	maxServers  int
}

type md5PoolDigest struct {
	parent *md5PoolServer
	client Hasher
	server Server
}

func NewPool() HashPool {
	return NewPoolWithOptions(ServerOptions{UseAVX512: true})
}

func NewPoolWithOptions(opts ServerOptions) HashPool {
	return &md5PoolServer{options: opts, maxServers: 32}
}

func (s *md5PoolServer) swap(i, j int) {
	tmp := s.servers[j]
	s.servers[j] = s.servers[i]
	s.servers[i] = tmp
	tmpcl := s.freeClients[j]
	s.freeClients[j] = s.freeClients[i]
	s.freeClients[i] = tmpcl
}

func (s *md5PoolServer) Get() Hasher {
	s.mu.Lock()
	var r *md5PoolDigest
	for i := 0; i < len(s.servers); i++ {
		if len(s.freeClients[i]) > 0 {
			r = s.freeClients[i][len(s.freeClients[i])-1]
			s.freeClients[i] = s.freeClients[i][0 : len(s.freeClients[i])-1]
			if i > 0 && len(s.freeClients[i]) < len(s.freeClients[i-1]) {
				// Always move more occupied servers closer to the beginning
				j := i - 1
				for j >= 0 && len(s.freeClients[i]) < len(s.freeClients[j]) {
					j--
				}
				s.swap(i, j)
			}
			break
		}
	}
	if r == nil {
		srv := NewServerWithOptions(s.options)
		clients := make([]*md5PoolDigest, Lanes)
		for i := 0; i < Lanes; i++ {
			clients[i] = &md5PoolDigest{
				parent: s,
				client: srv.NewHash(),
				server: srv,
			}
		}
		s.servers = append(s.servers, srv)
		s.freeClients = append(s.freeClients, clients[0:len(clients)-1])
		r = clients[len(clients)-1]
	}
	s.mu.Unlock()
	return r
}

func (s *md5PoolServer) Put(client Hasher) (err error) {
	s.mu.Lock()
	if r, ok := client.(*md5PoolDigest); ok {
		r.Reset()
		i := 0
		for ; i < len(s.servers); i++ {
			if s.servers[i] == r.server {
				s.freeClients[i] = append(s.freeClients[i], r)
				if len(s.freeClients[i]) == Lanes && len(s.servers) > s.maxServers {
					for _, cl := range s.freeClients[i] {
						cl.client.Close()
					}
					s.servers[i].Close()
					s.servers = append(s.servers[0:i], s.servers[i+1:]...)
					s.freeClients = append(s.freeClients[0:i], s.freeClients[i+1:]...)
				} else if i < len(s.servers)-1 && len(s.freeClients[i]) > len(s.freeClients[i+1]) {
					// Always move more occupied servers closer to the beginning
					j := i + 1
					for j < len(s.servers)-1 && len(s.freeClients[i]) > len(s.freeClients[j]) {
						j++
					}
					s.swap(i, j)
				}
				break
			}
		}
		if i >= len(s.servers) {
			err = fmt.Errorf("Put(): hasher not from this pool")
		}
	} else {
		err = fmt.Errorf("Put(): hasher not from a pool")
	}
	s.mu.Unlock()
	return
}

func (s *md5PoolServer) Close() {
	s.mu.Lock()
	for i := 0; i < len(s.servers); i++ {
		s.servers[i].Close()
	}
	s.mu.Unlock()
}

func (d *md5PoolDigest) Size() int {
	return d.client.Size()
}

func (d *md5PoolDigest) BlockSize() int {
	return d.client.BlockSize()
}

func (d *md5PoolDigest) Reset() {
	d.client.Reset()
}

func (d *md5PoolDigest) Write(p []byte) (nn int, err error) {
	nn, err = d.client.Write(p)
	return
}

func (d *md5PoolDigest) Sum(in []byte) (result []byte) {
	return d.client.Sum(in)
}

func (d *md5PoolDigest) Close() {
	d.parent.Put(d)
}
