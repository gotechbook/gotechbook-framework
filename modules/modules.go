package modules

import (
	"encoding/json"
	"errors"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"os"
)

type Module interface {
	Init() error
	AfterInit()
	BeforeShutdown()
	Shutdown() error
}

var (
	ErrTimeoutTerminatingBinaryModule = errors.New("timeout waiting to binary module to die")
)

type Server struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Metadata map[string]string `json:"metadata"`
	Frontend bool              `json:"frontend"`
	Hostname string            `json:"hostname"`
}

func NewServer(id, serverType string, frontend bool, metadata ...map[string]string) *Server {
	d := make(map[string]string)
	h, err := os.Hostname()
	if err != nil {
		logger.Log.Errorf("failed to get hostname: %s", err.Error())
	}
	if len(metadata) > 0 {
		d = metadata[0]
	}
	return &Server{
		ID:       id,
		Type:     serverType,
		Metadata: d,
		Frontend: frontend,
		Hostname: h,
	}
}

func (s *Server) AsJSONString() string {
	str, err := json.Marshal(s)
	if err != nil {
		logger.Log.Errorf("error getting server as json: %s", err.Error())
		return ""
	}
	return string(str)
}
