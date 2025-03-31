package etcdiscov

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ringbrew/gsv/discovery"
	"github.com/ringbrew/gsv/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type EtcdDiscovery struct {
	client            *clientv3.Client
	prefix            string
	keepAliveInterval int64
	nodeCancel        sync.Map
	sync.RWMutex
}

type Option struct {
	Endpoint          []string `yaml:"endpoint"`
	Prefix            string   `yaml:"prefix"`
	KeepAliveInterval int64    `yaml:"keep_alive_interval"`
}

type commandLogger struct{}

func (c *commandLogger) UnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	logger.Debug(logger.NewEntry(ctx).WithMessageF("sending cmd to etcd server, method[%s], req param[%v]", method, req))
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return err
	}
	logger.Debug(logger.NewEntry(ctx).WithMessageF("sending cmd to etcd server, method[%s], resp param[%v]", method, reply))
	return nil
}

const leaseIdKey = "etcd_lease_id"

func NewEtcdDiscovery(opt Option) (*EtcdDiscovery, error) {
	grpcOpts := make([]grpc.DialOption, 0)

	interceptor := &commandLogger{}
	grpcOpts = append(grpcOpts, grpc.WithUnaryInterceptor(interceptor.UnaryInterceptor))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   opt.Endpoint,
		DialTimeout: 5 * time.Second,
		DialOptions: grpcOpts,
	})
	if err != nil {
		return nil, err
	}

	prefix := "gsv"

	if opt.Prefix != "" {
		prefix = opt.Prefix
	}

	keepAliveInterval := int64(30)
	if opt.KeepAliveInterval != 0 {
		keepAliveInterval = opt.KeepAliveInterval
	}

	result := &EtcdDiscovery{
		client:            client,
		prefix:            prefix,
		keepAliveInterval: keepAliveInterval,
	}

	return result, nil
}

func (e *EtcdDiscovery) nodePath(name string, nodeType discovery.Type, tag ...string) string {
	if len(tag) > 0 && tag[0] != "" {
		return fmt.Sprintf("/%s/%s/%s/%s", e.prefix, name, tag[0], nodeType)
	}
	return fmt.Sprintf("/%s/%s/%s", e.prefix, name, nodeType)
}

func (e *EtcdDiscovery) nodeKey(node *discovery.Node) string {
	if node.Tag != "" {
		return fmt.Sprintf("/%s/%s/%s/%s/%s", e.prefix, node.Name, node.Tag, node.Type, node.Id)
	}
	return fmt.Sprintf("/%s/%s/%s/%s", e.prefix, node.Name, node.Type, node.Id)
}

func (e *EtcdDiscovery) encodeNode(node *discovery.Node) string {
	val, _ := json.Marshal(node)
	return string(val)
}

func (e *EtcdDiscovery) Node(name string, nodeType discovery.Type, tag ...string) ([]*discovery.Node, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, e.nodePath(name, nodeType, tag...), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make([]*discovery.Node, 0)

	for _, event := range resp.Kvs {
		node := discovery.Node{}
		if err := json.NewDecoder(bytes.NewReader(event.Value)).Decode(&node); err != nil {
			logger.Error(logger.NewEntry().WithMessage(fmt.Sprintf("etcd discovery node decode error:%s", err.Error())))
			continue
		}
		result = append(result, &node)
	}

	return result, nil
}

func (e *EtcdDiscovery) Watch(name string, nodeType discovery.Type, tag ...string) (chan discovery.NodeEvent, error) {
	result := make(chan discovery.NodeEvent)
	watcher := clientv3.NewWatcher(e.client)

	go func() {
		defer func() {
			if p := recover(); p != nil {
				logger.Error(logger.NewEntry().WithMessage(fmt.Sprintf("etcd discovery watch[%s][%s] panic:%v", name, nodeType, p)))
			}
		}()

		watchChan := watcher.Watch(context.Background(), e.nodePath(name, nodeType, tag...), clientv3.WithPrefix(), clientv3.WithPrevKV())
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					node := discovery.Node{}
					if err := json.NewDecoder(bytes.NewReader(event.Kv.Value)).Decode(&node); err != nil {
						logger.Error(logger.NewEntry().WithMessage(fmt.Sprintf("etcd discovery put node decode error:%s", err.Error())))
						continue
					}

					nodeEvent := discovery.NodeEvent{
						Event: discovery.NodeEventAdd,
						Node:  []*discovery.Node{&node},
					}

					result <- nodeEvent
				case mvccpb.DELETE:
					node := discovery.Node{}
					if err := json.NewDecoder(bytes.NewReader(event.PrevKv.Value)).Decode(&node); err != nil {
						logger.Error(logger.NewEntry().WithMessage(fmt.Sprintf("etcd discovery delete node decode error:%s", err.Error())))
						continue
					}

					nodeEvent := discovery.NodeEvent{
						Event: discovery.NodeEventRemove,
						Node:  []*discovery.Node{&node},
					}

					result <- nodeEvent
				}
			}
		}
	}()

	return result, nil
}

func (e *EtcdDiscovery) Register(node *discovery.Node) error {
	kv := clientv3.NewKV(e.client)
	lease := clientv3.NewLease(e.client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaseResp, err := lease.Grant(ctx, e.keepAliveInterval)
	if err != nil {
		return err
	}

	node.Extra.Store(leaseIdKey, leaseResp.ID)

	nodePath := e.nodeKey(node)

	if _, err := kv.Put(context.TODO(), nodePath, e.encodeNode(node), clientv3.WithLease(leaseResp.ID)); err != nil {
		return err
	}

	return nil
}
func (e *EtcdDiscovery) KeepAlive(node *discovery.Node) error {
	ctx, cancel := context.WithCancel(context.Background())
	e.nodeCancel.Store(node.Id, cancel)

	ticker := time.NewTicker((time.Duration(e.keepAliveInterval) * time.Second) / 3)
	for range ticker.C {
		id, exist := node.Extra.Load(leaseIdKey)
		if !exist {
			return errors.New("lease id not found")
		}

		leaseId, ok := id.(clientv3.LeaseID)
		if !ok {
			return fmt.Errorf("invalid lease id: %v", id)
		}

		resp, err := e.client.KeepAliveOnce(ctx, leaseId)
		if err != nil {
			logger.Error(logger.NewEntry().WithMessageF("keep alive lease[%d] error[%s]", leaseId, err.Error()))
			if err := e.Register(node); err != nil {
				logger.Error(logger.NewEntry().WithMessageF("keep alive register again node[%s] error[%s]", e.nodeKey(node), err.Error()))
			}
		} else {
			node.Extra.Store(leaseIdKey, resp.ID)
		}
	}

	return nil
}
func (e *EtcdDiscovery) Deregister(node *discovery.Node) error {
	if c, exist := e.nodeCancel.Load(node.Id); exist {
		if cancel, ok := c.(context.CancelFunc); ok {
			cancel()
		}
	}

	if id, exist := node.Extra.Load(leaseIdKey); exist {
		if leaseId, ok := id.(clientv3.LeaseID); ok {
			if _, err := e.client.Revoke(context.Background(), leaseId); err != nil {
				return err
			}
		}
	}

	return nil
}
