package jjgrpc_consul

import (
	"fmt"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc/resolver"
)

//
// Builder
//

func RegisterResolver() error {
	r, err := NewResolverBuilder()
	if err != nil {
		return err
	}
	resolver.Register(r)
	return nil
}

func NewResolverBuilder() (resolver.Builder, error) {
	consulcli, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return nil, err
	}

	return &consulResolverBuilder{
		consulcli: consulcli,
	}, nil
}

type consulResolverBuilder struct {
	consulcli *consul.Client
}

func (b *consulResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &consulResolver{
		consulcli: b.consulcli,
		freq:      time.Minute * 1,
		target:    target.Authority,
		ctx:       ctx,
		cancel:    cancel,
		cc:        cc,
		t:         time.NewTimer(0),
		rn:        make(chan struct{}, 1),
	}

	r.wg.Add(1)
	go r.watcher()
	return r, nil
}

func (b *consulResolverBuilder) Scheme() string {
	return "consul"
}

//
// Resolver
//

type consulResolver struct {
	consulcli *consul.Client
	freq      time.Duration
	target    string
	ctx       context.Context
	cancel    context.CancelFunc
	cc        resolver.ClientConn
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	rn chan struct{}
	t  *time.Timer
	// wg is used to enforce Close() to return after the watcher() goroutine has finished.
	// Otherwise, data race will be possible. [Race Example] in dns_resolver_test we
	// replace the real lookup functions with mocked ones to facilitate testing.
	// If Close() doesn't wait for watcher() goroutine finishes, race detector sometimes
	// will warns lookup (READ the lookup function pointers) inside watcher() goroutine
	// has data race with replaceNetFunc (WRITE the lookup function pointers).
	wg sync.WaitGroup
}

func (r *consulResolver) ResolveNow(opt resolver.ResolveNowOption) {
	select {
	case r.rn <- struct{}{}:
	default:
	}
}

func (r *consulResolver) Close() {
	r.cancel()
	r.wg.Wait()
	r.t.Stop()
}

func (r *consulResolver) watcher() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.t.C:
		case <-r.rn:
		}
		result := r.lookup()
		// Next lookup should happen after an interval defined by d.freq.
		r.t.Reset(r.freq)
		r.cc.NewAddress(result)
	}
}

func (r *consulResolver) lookup() []resolver.Address {
	// query consul
	cs, _, err := r.consulcli.Health().Service(r.target, "", true, nil)
	if err != nil {
		return nil
	}

	addrs := make([]resolver.Address, 0)
	for _, c := range cs {
		addrs = append(addrs, resolver.Address{
			Addr: fmt.Sprintf("%s:%d", c.Node.Address, c.Service.Port),
		})
	}

	return addrs
}
