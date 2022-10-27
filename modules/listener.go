package modules

type SDListener interface {
	AddServer(*Server)
	RemoveServer(*Server)
}
