package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/golang/protobuf/proto"
	config "github.com/gotechbook/gotechbook-framework-config"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"github.com/topfreegames/go-workers"
	"os"
)

const (
	rpcQueue = "rpc"
	class    = ""
)

var (
	ErrRPCJobAlreadyRegistered = errors.New("rpc job was already registered")
)

type rpcInfo struct {
	Route    string
	Metadata map[string]interface{}
	Arg      proto.Message
	Reply    proto.Message
}
type rpcRoute struct {
	Route string
}

var _ Worker = (*GoWorker)(nil)

type Worker interface {
	Start()
	Started() bool
	Register(job Job) error
	EnqueueRPC(routeStr string, metadata map[string]interface{}, reply, arg proto.Message) (string, error)
	EnqueueRPCWithOptions(routeStr string, metadata map[string]interface{}, reply, arg proto.Message, enabled bool, max int, Exponential int, minDelay int, maxDelay int, maxRandom int) (string, error)
}
type Job interface {
	Discovery(route string, rpcMetadata map[string]interface{}) (serverID string, err error)
	RPC(ctx context.Context, serverID, routeStr string, reply, arg proto.Message) error
	GetArgReply(route string) (arg, reply proto.Message, err error)
}

type GoWorker struct {
	concurrency int
	registered  bool
	started     bool
	Enabled     bool
	Max         int
	Exponential int
	MinDelay    int
	MaxDelay    int
	MaxRandom   int
}

func NewGoWorker(config config.Worker) (*GoWorker, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	workers.Configure(map[string]string{
		"server":    config.GoTechBookFrameworkWorkerRedisUrl,
		"pool":      config.GoTechBookFrameworkWorkerRedisPool,
		"password":  config.GoTechBookFrameworkWorkerRedisPassword,
		"namespace": config.GoTechBookFrameworkWorkerNamespace,
		"process":   hostname,
	})
	return &GoWorker{
		concurrency: config.GoTechBookFrameworkWorkerConcurrency,
		Enabled:     config.GoTechBookFrameworkWorkerRetryEnabled,
		Max:         config.GoTechBookFrameworkWorkerRetryMax,
		Exponential: config.GoTechBookFrameworkWorkerRetryExponential,
		MinDelay:    config.GoTechBookFrameworkWorkerRetryMinDelay,
		MaxDelay:    config.GoTechBookFrameworkWorkerRetryMaxDelay,
		MaxRandom:   config.GoTechBookFrameworkWorkerRetryMaxRandom,
	}, nil
}

func (w *GoWorker) Start() {
	go workers.Start()
	w.started = true
}
func (w *GoWorker) Started() bool {
	return w != nil && w.started
}
func (w *GoWorker) Register(job Job) error {
	if w.registered {
		return ErrRPCJobAlreadyRegistered
	}
	workers.Process(rpcQueue, w.parsedRPCJob(job), w.concurrency)
	w.registered = true
	return nil
}
func (w *GoWorker) EnqueueRPC(routeStr string, metadata map[string]interface{}, reply, arg proto.Message) (jid string, err error) {
	opts := workers.EnqueueOptions{
		Retry:    w.Enabled,
		RetryMax: w.Max,
		RetryOptions: workers.RetryOptions{
			Exp:      w.Exponential,
			MinDelay: w.MinDelay,
			MaxDelay: w.MaxDelay,
			MaxRand:  w.MaxRandom,
		},
	}
	return workers.EnqueueWithOptions(rpcQueue, class, &rpcInfo{
		Route:    routeStr,
		Metadata: metadata,
		Arg:      arg,
		Reply:    reply,
	}, opts)
}
func (w *GoWorker) EnqueueRPCWithOptions(routeStr string, metadata map[string]interface{}, reply, arg proto.Message, enabled bool, max int, Exponential int, minDelay int, maxDelay int, maxRandom int) (jid string, err error) {
	return workers.EnqueueWithOptions(rpcQueue, class, &rpcInfo{
		Route:    routeStr,
		Metadata: metadata,
		Arg:      arg,
		Reply:    reply,
	}, workers.EnqueueOptions{
		Retry:    enabled,
		RetryMax: max,
		RetryOptions: workers.RetryOptions{
			Exp:      Exponential,
			MinDelay: minDelay,
			MaxDelay: maxDelay,
			MaxRand:  maxRandom,
		},
	})
}
func (w *GoWorker) SetLogger(logger logger.Logger) {
	workers.Logger = logger
}
func (w *GoWorker) parsedRPCJob(rpcJob Job) func(*workers.Msg) {
	return func(jobArg *workers.Msg) {
		logger.Log.Debug("executing rpc job")
		bts, rpcRoute, err := w.unmarshalRouteMetadata(jobArg)
		if err != nil {
			logger.Log.Errorf("failed to get job arg: %q", err)
			panic(err)
		}
		logger.Log.Debug("getting route arg and reply")
		arg, reply, err := rpcJob.GetArgReply(rpcRoute.Route)
		if err != nil {
			logger.Log.Errorf("failed to get methods arg and reply: %q", err)
			panic(err)
		}
		rpcInfo := &rpcInfo{
			Arg:   arg,
			Reply: reply,
		}
		logger.Log.Debug("unmarshalling rpc info")
		err = json.Unmarshal(bts, rpcInfo)
		if err != nil {
			logger.Log.Errorf("failed to unmarshal rpc info: %q", err)
			panic(err)
		}

		logger.Log.Debug("choosing server to make rpc")
		serverID, err := rpcJob.Discovery(rpcInfo.Route, rpcInfo.Metadata)
		if err != nil {
			logger.Log.Errorf("failed get server: %q", err)
			panic(err)
		}

		ctx := context.Background()

		logger.Log.Debugf("executing rpc func to %s", rpcInfo.Route)
		err = rpcJob.RPC(ctx, serverID, rpcInfo.Route, reply, arg)
		if err != nil {
			logger.Log.Errorf("failed make rpc: %q", err)
			panic(err)
		}

		logger.Log.Debug("finished executing rpc job")
	}
}
func (w *GoWorker) unmarshalRouteMetadata(jobArg *workers.Msg) ([]byte, *rpcRoute, error) {
	bts, err := jobArg.Args().MarshalJSON()
	if err != nil {
		return nil, nil, err
	}
	rpcRoute := new(rpcRoute)
	err = json.Unmarshal(bts, rpcRoute)
	if err != nil {
		return nil, nil, err
	}
	return bts, rpcRoute, nil
}
