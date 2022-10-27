package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	config "github.com/gotechbook/gotechbook-framework-config"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	util "github.com/gotechbook/gotechbook-framework-utils"
	"github.com/gotechbook/gotechbook-framework/grpc"
	"github.com/gotechbook/gotechbook-framework/modules"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"strings"
	"sync"
	"time"
)

var _ Discovery = (*discovery)(nil)

type Action int

const (
	ADD Action = iota
	DEL
)

type discovery struct {
	cli                    *v3.Client
	syncServersInterval    time.Duration
	heartbeatTTL           time.Duration
	logHeartbeat           bool
	lastHeartbeatTime      time.Time
	leaseID                v3.LeaseID
	mapByTypeLock          sync.RWMutex
	serverMapByType        map[string]map[string]*modules.Server
	serverMapByID          sync.Map
	etcdEndpoints          []string
	etcdUser               string
	etcdPass               string
	etcdPrefix             string
	etcdDialTimeout        time.Duration
	running                bool
	server                 *modules.Server
	stopChan               chan bool
	stopLeaseChan          chan bool
	lastSyncTime           time.Time
	listeners              []grpc.SDListener
	revokeTimeout          time.Duration
	grantLeaseTimeout      time.Duration
	grantLeaseMaxRetries   int
	grantLeaseInterval     time.Duration
	shutdownDelay          time.Duration
	appDieChan             chan bool
	serverTypesBlacklist   []string
	syncServersParallelism int
	syncServersRunning     chan bool
}

func NewDiscovery(config config.Cluster, server *modules.Server, appDieChan chan bool, cli ...*v3.Client) (Discovery, error) {
	var client *v3.Client
	if len(cli) > 0 {
		client = cli[0]
	}
	d := &discovery{
		running:            false,
		server:             server,
		serverMapByType:    make(map[string]map[string]*modules.Server),
		listeners:          make([]grpc.SDListener, 0),
		stopChan:           make(chan bool),
		stopLeaseChan:      make(chan bool),
		appDieChan:         appDieChan,
		cli:                client,
		syncServersRunning: make(chan bool),
	}
	d.configure(config)

	return d, nil
}
func (d *discovery) AddListener(listener grpc.SDListener) {
	d.listeners = append(d.listeners, listener)
}
func (d *discovery) AfterInit() {}
func (d *discovery) GetServersByType(serverType string) (map[string]*modules.Server, error) {
	d.mapByTypeLock.RLock()
	defer d.mapByTypeLock.RUnlock()
	if m, ok := d.serverMapByType[serverType]; ok && len(m) > 0 {
		// Create a new map to avoid concurrent read and write access to the
		// map, this also prevents accidental changes to the list of servers
		// kept by the service discovery.
		ret := make(map[string]*modules.Server, len(d.serverMapByType[serverType]))
		for k, v := range d.serverMapByType[serverType] {
			ret[k] = v
		}
		return ret, nil
	}
	return nil, ErrNoServersAvailableOfType
}
func (d *discovery) GetServers() []*modules.Server {
	ret := make([]*modules.Server, 0)
	d.serverMapByID.Range(func(k, v interface{}) bool {
		ret = append(ret, v.(*modules.Server))
		return true
	})
	return ret
}
func (d *discovery) GetServer(id string) (*modules.Server, error) {
	if sv, ok := d.serverMapByID.Load(id); ok {
		return sv.(*modules.Server), nil
	}
	return nil, ErrNoServerWithID
}
func (d *discovery) Init() error {
	d.running = true
	var err error

	if d.cli == nil {
		err = d.InitETCDClient()
		if err != nil {
			return err
		}
	} else {
		d.cli.KV = namespace.NewKV(d.cli.KV, d.etcdPrefix)
		d.cli.Watcher = namespace.NewWatcher(d.cli.Watcher, d.etcdPrefix)
		d.cli.Lease = namespace.NewLease(d.cli.Lease, d.etcdPrefix)
	}
	go d.watchEtcdChanges()

	if err = d.bootstrap(); err != nil {
		return err
	}

	// update servers
	syncServersTicker := time.NewTicker(d.syncServersInterval)
	go func() {
		for d.running {
			select {
			case <-syncServersTicker.C:
				err := d.SyncServers(false)
				if err != nil {
					logger.Log.Errorf("error resyncing servers: %s", err.Error())
				}
			case <-d.stopChan:
				return
			}
		}
	}()

	return nil
}
func (d *discovery) SyncServers(firstSync bool) error {
	d.syncServersRunning <- true
	defer func() {
		d.syncServersRunning <- false
	}()
	start := time.Now()
	var kvs *v3.GetResponse
	var err error
	if firstSync {
		kvs, err = d.cli.Get(
			context.TODO(),
			"servers/",
			v3.WithPrefix(),
		)
	} else {
		kvs, err = d.cli.Get(
			context.TODO(),
			"servers/",
			v3.WithPrefix(),
			v3.WithKeysOnly(),
		)
	}
	if err != nil {
		logger.Log.Errorf("Error querying etcd server: %s", err.Error())
		return err
	}

	// delete invalid servers (local ones that are not in etcd)
	var allIds = make([]string, 0)

	// Spawn worker goroutines that will work in parallel
	parallelGetter := newParallelGetter(d.cli, d.syncServersParallelism)

	for _, kv := range kvs.Kvs {
		svType, svID, err := parseEtcdKey(string(kv.Key))
		if err != nil {
			logger.Log.Warnf("failed to parse etcd key %s, error: %s", kv.Key, err.Error())
			continue
		}

		// Check whether the server type is blacklisted or not
		if d.isServerTypeBlacklisted(svType) && svID != d.server.ID {
			logger.Log.Debug("ignoring blacklisted server type '%s'", svType)
			continue
		}

		allIds = append(allIds, svID)

		if _, ok := d.serverMapByID.Load(svID); !ok {
			// Add new work to the channel
			if firstSync {
				parallelGetter.addWorkWithPayload(svType, svID, kv.Value)
			} else {
				parallelGetter.addWork(svType, svID)
			}
		}
	}

	// Wait until all goroutines are finished
	servers := parallelGetter.waitAndGetResult()

	for _, server := range servers {
		logger.Log.Debugf("adding server %s", server)
		d.addServer(server)
	}

	d.deleteLocalInvalidServers(allIds)

	d.printServers()
	d.lastSyncTime = time.Now()
	elapsed := time.Since(start)
	logger.Log.Infof("SyncServers took : %s to run", elapsed)
	return nil
}
func (d *discovery) BeforeShutdown() {
	d.revoke()
	time.Sleep(d.shutdownDelay) // Sleep for a short while to ensure shutdown has propagated
}
func (d *discovery) Shutdown() error {
	d.running = false
	close(d.stopChan)
	return nil
}
func (d *discovery) configure(config config.Cluster) {
	d.etcdEndpoints = config.GoTechBookFrameworkClusterSdEtcdEndpoints
	d.etcdUser = config.GoTechBookFrameworkClusterSdEtcdUser
	d.etcdPass = config.GoTechBookFrameworkClusterSdEtcdPass
	d.etcdDialTimeout = config.GoTechBookFrameworkClusterSdEtcdDialTimeout
	d.etcdPrefix = config.GoTechBookFrameworkClusterSdEtcdPrefix
	d.heartbeatTTL = config.GoTechBookFrameworkClusterSdEtcdHeartbeatTtl
	d.logHeartbeat = config.GoTechBookFrameworkClusterSdEtcdHeartbeatLog
	d.syncServersInterval = config.GoTechBookFrameworkClusterSdEtcdSyncServersInterval
	d.revokeTimeout = config.GoTechBookFrameworkClusterSdEtcdRevokeTimeout
	d.grantLeaseTimeout = config.GoTechBookFrameworkClusterSdEtcdGrantLeaseTimeout
	d.grantLeaseMaxRetries = config.GoTechBookFrameworkClusterSdEtcdGrantLeaseMaxRetries
	d.grantLeaseInterval = config.GoTechBookFrameworkClusterSdEtcdGrantLeaseRetryInterval
	d.shutdownDelay = config.GoTechBookFrameworkClusterSdEtcdShutdownDelay
	d.serverTypesBlacklist = config.GoTechBookFrameworkClusterSdEtcdServerTypeBlacklist
	d.syncServersParallelism = config.GoTechBookFrameworkClusterSdEtcdSyncServersParallelism
}
func (d *discovery) watchLeaseChan(c <-chan *v3.LeaseKeepAliveResponse) {
	failedGrantLeaseAttempts := 0
	for {
		select {
		case <-d.stopChan:
			return
		case <-d.stopLeaseChan:
			return
		case leaseKeepAliveResponse, ok := <-c:
			if !ok {
				logger.Log.Error("ETCD lease KeepAlive died, retrying in 10 seconds")
				time.Sleep(10000 * time.Millisecond)
			}
			if leaseKeepAliveResponse != nil {
				if d.logHeartbeat {
					logger.Log.Debugf("sd: etcd lease %x renewed", leaseKeepAliveResponse.ID)
				}
				failedGrantLeaseAttempts = 0
				continue
			}
			logger.Log.Warn("sd: error renewing etcd lease, reconfiguring")
			for {
				err := d.renewLease()
				if err != nil {
					failedGrantLeaseAttempts = failedGrantLeaseAttempts + 1
					if err == ErrEtcdGrantLeaseTimeout {
						logger.Log.Warn("sd: timed out trying to grant etcd lease")
						if d.appDieChan != nil {
							d.appDieChan <- true
						}
						return
					}
					if failedGrantLeaseAttempts >= d.grantLeaseMaxRetries {
						logger.Log.Warn("sd: exceeded max attempts to renew etcd lease")
						if d.appDieChan != nil {
							d.appDieChan <- true
						}
						return
					}
					logger.Log.Warnf("sd: error granting etcd lease, will retry in %d seconds", uint64(d.grantLeaseInterval.Seconds()))
					time.Sleep(d.grantLeaseInterval)
					continue
				}
				return
			}
		}
	}
}
func (d *discovery) renewLease() error {
	c := make(chan error)
	go func() {
		defer close(c)
		logger.Log.Infof("waiting for etcd lease")
		err := d.grantLease()
		if err != nil {
			c <- err
			return
		}
		err = d.bootstrapServer(d.server)
		c <- err
	}()
	select {
	case err := <-c:
		return err
	case <-time.After(d.grantLeaseTimeout):
		return ErrEtcdGrantLeaseTimeout
	}
}
func (d *discovery) grantLease() error {
	// grab lease
	l, err := d.cli.Grant(context.TODO(), int64(d.heartbeatTTL.Seconds()))
	if err != nil {
		return err
	}
	d.leaseID = l.ID
	logger.Log.Debugf("sd: got leaseID: %x", l.ID)
	// this will keep alive forever, when channel c is closed
	// it means we probably have to rebootstrap the lease
	c, err := d.cli.KeepAlive(context.TODO(), d.leaseID)
	if err != nil {
		return err
	}
	// need to receive here as per etcd docs
	<-c
	go d.watchLeaseChan(c)
	return nil
}
func (d *discovery) addServerIntoEtcd(server *modules.Server) error {
	_, err := d.cli.Put(
		context.TODO(),
		getKey(server.ID, server.Type),
		server.AsJSONString(),
		v3.WithLease(d.leaseID),
	)
	return err
}
func (d *discovery) bootstrapServer(server *modules.Server) error {
	if err := d.addServerIntoEtcd(server); err != nil {
		return err
	}

	d.SyncServers(true)
	return nil
}
func (d *discovery) notifyListeners(act Action, sv *modules.Server) {
	for _, l := range d.listeners {
		if act == DEL {
			l.RemoveServer(sv)
		} else if act == ADD {
			l.AddServer(sv)
		}
	}
}
func (d *discovery) writeLockScope(f func()) {
	d.mapByTypeLock.Lock()
	defer d.mapByTypeLock.Unlock()
	f()
}
func (d *discovery) deleteServer(serverID string) {
	if actual, ok := d.serverMapByID.Load(serverID); ok {
		sv := actual.(*modules.Server)
		d.serverMapByID.Delete(sv.ID)
		d.writeLockScope(func() {
			if svMap, ok := d.serverMapByType[sv.Type]; ok {
				delete(svMap, sv.ID)
			}
		})
		d.notifyListeners(DEL, sv)
	}
}
func (d *discovery) deleteLocalInvalidServers(actualServers []string) {
	d.serverMapByID.Range(func(key interface{}, value interface{}) bool {
		k := key.(string)
		if !util.SliceContainsString(actualServers, k) {
			logger.Log.Warnf("deleting invalid local server %s", k)
			d.deleteServer(k)
		}
		return true
	})
}
func (d *discovery) revoke() error {
	close(d.stopLeaseChan)
	c := make(chan error)
	defer close(c)
	go func() {
		logger.Log.Debug("waiting for etcd revoke")
		_, err := d.cli.Revoke(context.TODO(), d.leaseID)
		c <- err
		logger.Log.Debug("finished waiting for etcd revoke")
	}()
	select {
	case err := <-c:
		return err // completed normally
	case <-time.After(d.revokeTimeout):
		logger.Log.Warn("timed out waiting for etcd revoke")
		return nil // timed out
	}
}
func (d *discovery) addServer(sv *modules.Server) {
	if _, loaded := d.serverMapByID.LoadOrStore(sv.ID, sv); !loaded {
		d.writeLockScope(func() {
			mapSvByType, ok := d.serverMapByType[sv.Type]
			if !ok {
				mapSvByType = make(map[string]*modules.Server)
				d.serverMapByType[sv.Type] = mapSvByType
			}
			mapSvByType[sv.ID] = sv
		})
		if sv.ID != d.server.ID {
			d.notifyListeners(ADD, sv)
		}
	}
}
func (d *discovery) watchEtcdChanges() {
	w := d.cli.Watch(context.Background(), "servers/", v3.WithPrefix())
	failedWatchAttempts := 0
	go func(chn v3.WatchChan) {
		for d.running {
			select {
			// Block here if SyncServers() is running and consume the watcher channel after it's finished, to avoid conflicts
			case syncServersState := <-d.syncServersRunning:
				for syncServersState {
					syncServersState = <-d.syncServersRunning
				}
			case wResp, ok := <-chn:
				if wResp.Err() != nil {
					logger.Log.Warnf("etcd watcher response error: %s", wResp.Err())
					time.Sleep(100 * time.Millisecond)
				}
				if !ok {
					logger.Log.Error("etcd watcher died, retrying to watch in 1 second")
					failedWatchAttempts++
					time.Sleep(1000 * time.Millisecond)
					if failedWatchAttempts > 10 {
						if err := d.InitETCDClient(); err != nil {
							failedWatchAttempts = 0
							continue
						}
						chn = d.cli.Watch(context.Background(), "servers/", v3.WithPrefix())
						failedWatchAttempts = 0
					}
					continue
				}
				failedWatchAttempts = 0
				for _, ev := range wResp.Events {
					svType, svID, err := parseEtcdKey(string(ev.Kv.Key))
					if err != nil {
						logger.Log.Warnf("failed to parse key from etcd: %s", ev.Kv.Key)
						continue
					}

					if d.isServerTypeBlacklisted(svType) && d.server.ID != svID {
						continue
					}

					switch ev.Type {
					case v3.EventTypePut:
						var sv *modules.Server
						var err error
						if sv, err = parseServer(ev.Kv.Value); err != nil {
							logger.Log.Errorf("Failed to parse server from etcd: %v", err)
							continue
						}

						d.addServer(sv)
						logger.Log.Debugf("server %s added by watcher", ev.Kv.Key)
						d.printServers()
					case v3.EventTypeDelete:
						d.deleteServer(svID)
						logger.Log.Debugf("server %s deleted by watcher", svID)
						d.printServers()
					}
				}
			case <-d.stopChan:
				return
			}

		}
	}(w)
}
func (d *discovery) isServerTypeBlacklisted(svType string) bool {
	for _, blacklistedSv := range d.serverTypesBlacklist {
		if blacklistedSv == svType {
			return true
		}
	}
	return false
}
func (d *discovery) printServers() {
	d.mapByTypeLock.RLock()
	defer d.mapByTypeLock.RUnlock()
	for k, v := range d.serverMapByType {
		logger.Log.Debugf("type: %s, servers: %+v", k, v)
	}
}
func (d *discovery) bootstrap() error {
	if err := d.grantLease(); err != nil {
		return err
	}
	if err := d.bootstrapServer(d.server); err != nil {
		return err
	}
	return nil
}
func (d *discovery) InitETCDClient() error {
	logger.Log.Infof("Initializing ETCD client")
	var cli *v3.Client
	var err error
	config := v3.Config{
		Endpoints:   d.etcdEndpoints,
		DialTimeout: d.etcdDialTimeout,
	}
	if d.etcdUser != "" && d.etcdPass != "" {
		config.Username = d.etcdUser
		config.Password = d.etcdPass
	}
	cli, err = v3.New(config)
	if err != nil {
		logger.Log.Errorf("error initializing etcd client: %s", err.Error())
		return err
	}
	d.cli = cli
	d.cli.KV = namespace.NewKV(d.cli.KV, d.etcdPrefix)
	d.cli.Watcher = namespace.NewWatcher(d.cli.Watcher, d.etcdPrefix)
	d.cli.Lease = namespace.NewLease(d.cli.Lease, d.etcdPrefix)
	return nil
}
func getKey(serverID, serverType string) string {
	return fmt.Sprintf("servers/%s/%s", serverType, serverID)
}
func getServerFromEtcd(cli *v3.Client, serverType, serverID string) (*modules.Server, error) {
	svKey := getKey(serverID, serverType)
	svEInfo, err := cli.Get(context.TODO(), svKey)
	if err != nil {
		return nil, fmt.Errorf("error getting server: %s from etcd, error: %s", svKey, err.Error())
	}
	if len(svEInfo.Kvs) == 0 {
		return nil, fmt.Errorf("didn't found server: %s in etcd", svKey)
	}
	return parseServer(svEInfo.Kvs[0].Value)
}
func parseEtcdKey(key string) (string, string, error) {
	slice := strings.Split(key, "/")
	if len(slice) != 3 {
		return "", "", fmt.Errorf("error parsing etcd key %s (server name can't contain /)", key)
	}
	svType := slice[1]
	svID := slice[2]
	return svType, svID, nil
}
func parseServer(value []byte) (*modules.Server, error) {
	var sv *modules.Server
	err := json.Unmarshal(value, &sv)
	if err != nil {
		logger.Log.Warnf("failed to load server %s, error: %s", sv, err.Error())
		return nil, err
	}
	return sv, nil
}

type parallelGetterWork struct {
	serverType string
	serverID   string
	payload    []byte
}
type parallelGetter struct {
	cli         *v3.Client
	numWorkers  int
	wg          *sync.WaitGroup
	resultMutex sync.Mutex
	result      *[]*modules.Server
	workChan    chan parallelGetterWork
}

func newParallelGetter(cli *v3.Client, numWorkers int) parallelGetter {
	if numWorkers <= 0 {
		numWorkers = 10
	}
	p := parallelGetter{
		cli:        cli,
		numWorkers: numWorkers,
		workChan:   make(chan parallelGetterWork),
		wg:         new(sync.WaitGroup),
		result:     new([]*modules.Server),
	}
	p.start()
	return p
}
func (p *parallelGetter) start() {
	for i := 0; i < p.numWorkers; i++ {
		go func() {
			for work := range p.workChan {
				logger.Log.Debugf("loading info from missing server: %s/%s", work.serverType, work.serverID)
				var sv *modules.Server
				var err error
				if work.payload == nil {
					sv, err = getServerFromEtcd(p.cli, work.serverType, work.serverID)
				} else {
					sv, err = parseServer(work.payload)
				}
				if err != nil {
					logger.Log.Errorf("Error parsing server from etcd: %s, error: %s", work.serverID, err.Error())
					p.wg.Done()
					continue
				}

				p.resultMutex.Lock()
				*p.result = append(*p.result, sv)
				p.resultMutex.Unlock()

				p.wg.Done()
			}
		}()
	}
}
func (p *parallelGetter) waitAndGetResult() []*modules.Server {
	p.wg.Wait()
	close(p.workChan)
	return *p.result
}
func (p *parallelGetter) addWorkWithPayload(serverType, serverID string, payload []byte) {
	p.wg.Add(1)
	p.workChan <- parallelGetterWork{
		serverType: serverType,
		serverID:   serverID,
		payload:    payload,
	}
}
func (p *parallelGetter) addWork(serverType, serverID string) {
	p.wg.Add(1)
	p.workChan <- parallelGetterWork{
		serverType: serverType,
		serverID:   serverID,
	}
}
