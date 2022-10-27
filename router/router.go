package router

import (
	"context"
	"errors"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/message"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/route"
	"math/rand"
	"time"
)

var (
	ErrServiceDiscoveryNotInitialized = errors.New("service discovery client is not initialized")
)

// Router struct
type Router struct {
	serviceDiscovery modules.Discovery
	routesMap        map[string]RoutingFunc
}

// RoutingFunc defines a routing function
type RoutingFunc func(
	ctx context.Context,
	route *route.Route,
	payload []byte,
	servers map[string]*modules.Server,
) (*modules.Server, error)

// New returns the router
func New() *Router {
	return &Router{
		routesMap: make(map[string]RoutingFunc),
	}
}

// SetServiceDiscovery sets the sd client
func (r *Router) SetServiceDiscovery(sd modules.Discovery) {
	r.serviceDiscovery = sd
}

func (r *Router) defaultRoute(
	servers map[string]*modules.Server,
) *modules.Server {
	srvList := make([]*modules.Server, 0)
	s := rand.NewSource(time.Now().Unix())
	rnd := rand.New(s)
	for _, v := range servers {
		srvList = append(srvList, v)
	}
	server := srvList[rnd.Intn(len(srvList))]
	return server
}

// Route gets the right server to use in the call
func (r *Router) Route(
	ctx context.Context,
	rpcType proto.RPCType,
	svType string,
	route *route.Route,
	msg *message.Message,
) (*modules.Server, error) {
	if r.serviceDiscovery == nil {
		return nil, ErrServiceDiscoveryNotInitialized
	}
	serversOfType, err := r.serviceDiscovery.GetServersByType(svType)
	if err != nil {
		return nil, err
	}
	if rpcType == proto.RPCType_User {
		server := r.defaultRoute(serversOfType)
		return server, nil
	}
	routeFunc, ok := r.routesMap[svType]
	if !ok {
		logger.Log.Debugf("no specific route for svType: %s, using default route", svType)
		server := r.defaultRoute(serversOfType)
		return server, nil
	}
	return routeFunc(ctx, route, msg.Data, serversOfType)
}

// AddRoute adds a routing function to a server type
func (r *Router) AddRoute(
	serverType string,
	routingFunction RoutingFunc,
) {
	if _, ok := r.routesMap[serverType]; ok {
		logger.Log.Warnf("overriding the route to svType %s", serverType)
	}
	r.routesMap[serverType] = routingFunction
}
