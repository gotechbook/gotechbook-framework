package component

var _ Component = (*Base)(nil)

type Base struct{}

func (c *Base) Init()           {}
func (c *Base) AfterInit()      {}
func (c *Base) BeforeShutdown() {}
func (c *Base) Shutdown()       {}
