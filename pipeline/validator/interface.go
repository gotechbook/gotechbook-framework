package validator

import "context"

type StructValidator interface {
	Validate(context.Context, interface{}) (context.Context, interface{}, error)
}

// StructValidatorInstance holds the default validator
// on start but can be overridden if needed.
var StructValidatorInstance StructValidator = &DefaultValidator{}
