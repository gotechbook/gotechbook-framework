package grpc

import (
	"fmt"
	config "github.com/gotechbook/gotechbook-framework-config"
	metrics "github.com/gotechbook/gotechbook-framework-metrics"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/modules"
	"google.golang.org/grpc"
	"net"
)

var _ RPCServer = (*GServer)(nil)

type GServer struct {
	server           *modules.Server
	port             int
	metricsReporters []metrics.Reporter
	grpcSv           *grpc.Server
	appServer        proto.AppServer
}

func NewGServer(config config.Cluster, server *modules.Server, metricsReporters []metrics.Reporter) (*GServer, error) {
	gs := &GServer{
		port:             config.GoTechBookFrameworkClusterRpcServerGrpcPort,
		server:           server,
		metricsReporters: metricsReporters,
	}
	return gs, nil
}

func (s *GServer) SetServer(server proto.AppServer) {
	s.appServer = server
}
func (s *GServer) Init() error {
	port := s.port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	s.grpcSv = grpc.NewServer()
	proto.RegisterAppServer(s.grpcSv, s.appServer)
	go s.grpcSv.Serve(lis)
	return nil
}
func (s *GServer) AfterInit()      {}
func (s *GServer) BeforeShutdown() {}
func (s *GServer) Shutdown() error {
	s.grpcSv.GracefulStop()
	return nil
}
