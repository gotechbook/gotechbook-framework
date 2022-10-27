package modules

type Base struct{}

func (c *Base) Init() error {
	return nil
}
func (c *Base) AfterInit()      {}
func (c *Base) BeforeShutdown() {}
func (c *Base) Shutdown() error {
	return nil
}
