package router

import (
	"context"
	"errors"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/route"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	serverID   = "id"
	serverType = "serverType"
	frontend   = true
	server     = modules.NewServer(serverID, serverType, frontend)
	servers    = map[string]*modules.Server{
		serverID: server,
	}

	routingFunction = func(
		ctx context.Context,
		route *route.Route,
		payload []byte,
		servers map[string]*modules.Server,
	) (*modules.Server, error) {
		return server, nil
	}
)

var routerTables = map[string]struct {
	server     *modules.Server
	serverType string
	rpcType    proto.RPCType
	err        error
}{
	"test_server_has_route_func":   {server, serverType, proto.RPCType_Sys, nil},
	"test_server_use_default_func": {server, "notRegisteredType", proto.RPCType_Sys, nil},
	"test_user_use_default_func":   {server, serverType, proto.RPCType_User, nil},
	"test_error_on_service_disc":   {nil, serverType, proto.RPCType_Sys, errors.New("sd error")},
}

var addRouteRouterTables = map[string]struct {
	serverType string
}{
	"test_overrige_server_type": {serverType},
	"test_new_server_type":      {"notRegisteredType"},
}

func TestNew(t *testing.T) {
	t.Parallel()
	router := New()
	assert.NotNil(t, router)
}

func TestDefaultRoute(t *testing.T) {
	t.Parallel()

	router := New()

	retServer := router.defaultRoute(servers)
	assert.Equal(t, server, retServer)
}

func TestAddRoute(t *testing.T) {
	t.Parallel()

	for name, table := range addRouteRouterTables {
		t.Run(name, func(t *testing.T) {
			router := New()
			router.AddRoute(table.serverType, routingFunction)

			assert.NotNil(t, router.routesMap[table.serverType])
			assert.Nil(t, router.routesMap["anotherServerType"])
		})
	}
}
