package storage

type BindingStorage interface {
	GetUserFrontendID(uid, frontendType string) (string, error)
	PutBinding(uid string) error
}
