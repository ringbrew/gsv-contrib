package confdiscov

import (
	"github.com/ringbrew/gsv/discovery"
	"sync"
)

type NodeInfo struct {
	Name string
	Type discovery.Type
}

type Discovery struct {
	data map[NodeInfo][]discovery.Node
	sync.RWMutex
}

func New(data map[NodeInfo][]discovery.Node) discovery.Discovery {
	return &Discovery{
		data: data,
	}
}

func (d *Discovery) ServiceNode(name string, nodeType discovery.Type) []discovery.Node {
	d.RLock()
	defer d.RUnlock()
	nodeList := d.data[NodeInfo{
		Name: name,
		Type: nodeType,
	}]

	if nodeList == nil {
		return []discovery.Node{}
	} else {
		return nodeList
	}
}
