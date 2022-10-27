package discovery

import (
	"errors"
	"github.com/gotechbook/gotechbook-framework/grpc"
	"github.com/gotechbook/gotechbook-framework/modules"
)

var (
	ErrNoServersAvailableOfType = errors.New("no servers available of this type")
	ErrNoServerWithID           = errors.New("can't find any server with the provided ID")
	ErrEtcdGrantLeaseTimeout    = errors.New("timed out waiting for etcd lease grant")
)

type Discovery interface {
	GetServersByType(serverType string) (map[string]*modules.Server, error)
	GetServer(id string) (*modules.Server, error)
	GetServers() []*modules.Server
	SyncServers(firstSync bool) error
	AddListener(listener grpc.SDListener)
	modules.Module
}
