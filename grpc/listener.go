package grpc

import "github.com/gotechbook/gotechbook-framework/modules"

type SDListener interface {
	AddServer(*modules.Server)
	RemoveServer(*modules.Server)
}
