package modules

type Discovery interface {
	GetServersByType(serverType string) (map[string]*Server, error)
	GetServer(id string) (*Server, error)
	GetServers() []*Server
	SyncServers(firstSync bool) error
	AddListener(listener SDListener)
	Module
}
