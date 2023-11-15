package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempt int = iota
	Retry
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy

	mutex sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
	b.mutex.Lock()
	b.Alive = alive
	b.mutex.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.Alive
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) AddBackend(b *Backend) {
	s.backends = append(s.backends, b)
}

func (s *ServerPool) HealthCheck() {
	for _, b := range s.backends {
		status := "up"
		alive := checkAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", b.URL, status)
	}
}

func (s *ServerPool) SetBackendAlive(url *url.URL, alive bool) {
	for _, b := range s.backends {
		if b.URL.String() == url.String() {
			b.SetAlive(alive)
			break
		}
	}
}

func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// GetNextPeer 返回下一个活跃的节点处理请求
func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndex()
	l := len(s.backends) + next
	for i := next; i < l; i++ {
		index := i % len(s.backends)
		if s.backends[index].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(index))
			}
			return s.backends[index]
		}
	}
	return nil
}

// checkAlive 通过是否能建立 TCP 链接判断节点是否存活
func checkAlive(u *url.URL) bool {
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)

	if err != nil {
		log.Println("Site unreachable: ", err)
		return false
	}

	defer func() {
		err = conn.Close()
		if err != nil {
			log.Println("Close conn error: ", err)
		}
	}()

	return true
}

// healthCheck 对注册的节点每 1 分钟执行一次健康检查
func healthCheck() {
	t := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-t.C:
			log.Println("Starting health check...")
			serverPool.HealthCheck()
			log.Println("Health check completed")
		}
	}
}

func GetAttemptFromContext(r *http.Request) int {
	if attempt, ok := r.Context().Value(Attempt).(int); ok {
		return attempt
	}
	return 0
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func lowbee(w http.ResponseWriter, r *http.Request) {
	attempt := GetAttemptFromContext(r)
	if attempt > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	peer := serverPool.GetNextPeer()
	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

var serverPool ServerPool

func main() {
	var servers string
	var port int

	flag.StringVar(&servers, "backends", "", "Load balanced backends, use commas to separate")
	flag.IntVar(&port, "port", 3030, "Port to serve")
	flag.Parse()

	if len(servers) == 0 {
		log.Fatal("No backend to load balance")
	}

	tokens := strings.Split(servers, ",")
	for _, token := range tokens {
		serverURL, err := url.Parse(token)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverURL)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, err error) {
			log.Printf("[%s] %s\n", serverURL.Host, err.Error())
			retry := GetRetryFromContext(request)
			if retry < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retry+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}
			// 3 次重试后，标记该节点不可用
			serverPool.SetBackendAlive(serverURL, false)

			// 重新进行下一轮 load balance
			attempt := GetAttemptFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempt)
			ctx := context.WithValue(request.Context(), Attempt, attempt+1)
			lowbee(writer, request.WithContext(ctx))
		}
		// 注册节点
		serverPool.AddBackend(&Backend{
			URL:          serverURL,
			Alive:        true,
			ReverseProxy: proxy,
		})
	}
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lowbee),
	}

	// 开始健康检查
	go healthCheck()

	log.Printf("Load Balancer started at: %d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
