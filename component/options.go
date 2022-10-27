package component

type (
	options struct {
		name     string
		nameFunc func(string) string
	}
	Option func(options *options)
)

func WithName(name string) Option {
	return func(opt *options) {
		opt.name = name
	}
}

func WithNameFunc(fn func(string) string) Option {
	return func(opt *options) {
		opt.nameFunc = fn
	}
}
