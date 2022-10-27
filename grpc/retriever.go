package grpc

import (
	config "github.com/gotechbook/gotechbook-framework-config"
)

type InfoRetriever interface {
	Region() string
}

type infoRetriever struct {
	region string
}

func NewInfoRetriever(config config.Cluster) InfoRetriever {
	return &infoRetriever{
		region: config.GoTechBookFrameworkClusterInfoRegion,
	}
}

func (c *infoRetriever) Region() string {
	return c.region
}
