package validator

import (
	"context"
	validator "github.com/go-playground/validator/v10"
	"sync"
)

type DefaultValidator struct {
	once     sync.Once
	validate *validator.Validate
}

// Validate is the the function responsible for validating the 'in' parameter
// based on the struct tags the parameter has.
// This function has the pipeline.Handler signature so
// it is possible to use it as a pipeline function
func (v *DefaultValidator) Validate(ctx context.Context, in interface{}) (context.Context, interface{}, error) {
	if in == nil {
		return ctx, in, nil
	}

	v.lazyInit()
	if err := v.validate.Struct(in); err != nil {
		return ctx, nil, err
	}

	return ctx, in, nil
}

func (v *DefaultValidator) lazyInit() {
	v.once.Do(func() {
		v.validate = validator.New()
	})
}
