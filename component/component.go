package component

import (
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/gotechbook/gotechbook-framework/message"
	"reflect"
	"unicode"
	"unicode/utf8"
)

type Component interface {
	Init()
	AfterInit()
	BeforeShutdown()
	Shutdown()
}

var (
	typeOfError    = reflect.TypeOf((*error)(nil)).Elem()
	typeOfBytes    = reflect.TypeOf(([]byte)(nil))
	typeOfContext  = reflect.TypeOf(new(context.Context)).Elem()
	typeOfProtoMsg = reflect.TypeOf(new(proto.Message)).Elem()
)
var (
	ErrRequestOnNotify = errors.New("tried to request a notify route")
	ErrNotifyOnRequest = errors.New("tried to notify a request route")
)

func isExported(name string) bool {
	w, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(w)
}

func isRemoteMethod(method reflect.Method) bool {
	mt := method.Type
	if method.PkgPath != "" {
		return false
	}
	if mt.NumIn() != 2 && mt.NumIn() != 3 {
		return false
	}
	if t1 := mt.In(1); !t1.Implements(typeOfContext) {
		return false
	}
	if mt.NumIn() == 3 {
		if t2 := mt.In(2); !t2.Implements(typeOfProtoMsg) {
			return false
		}
	}
	if mt.NumOut() != 2 {
		return false
	}
	if (mt.Out(0).Kind() != reflect.Ptr) || mt.Out(1) != typeOfError {
		return false
	}
	if o0 := mt.Out(0); !o0.Implements(typeOfProtoMsg) {
		return false
	}
	return true
}
func isHandlerMethod(method reflect.Method) bool {
	mt := method.Type
	// Method must be exported.
	if method.PkgPath != "" {
		return false
	}

	// Method needs two or three ins: receiver, context.Context and optional []byte or pointer.
	if mt.NumIn() != 2 && mt.NumIn() != 3 {
		return false
	}

	if t1 := mt.In(1); !t1.Implements(typeOfContext) {
		return false
	}

	if mt.NumIn() == 3 && mt.In(2).Kind() != reflect.Ptr && mt.In(2) != typeOfBytes {
		return false
	}

	// Method needs either no out or two outs: interface{}(or []byte), error
	if mt.NumOut() != 0 && mt.NumOut() != 2 {
		return false
	}

	if mt.NumOut() == 2 && (mt.Out(1) != typeOfError || mt.Out(0) != typeOfBytes && mt.Out(0).Kind() != reflect.Ptr) {
		return false
	}

	return true
}
func suitableRemoteMethods(typ reflect.Type, nameFunc func(string) string) map[string]*Remote {
	methods := make(map[string]*Remote)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mt := method.Type
		mn := method.Name
		if isRemoteMethod(method) {
			// rewrite remote name
			if nameFunc != nil {
				mn = nameFunc(mn)
			}
			methods[mn] = &Remote{
				Method:  method,
				HasArgs: method.Type.NumIn() == 3,
			}
			if mt.NumIn() == 3 {
				methods[mn].Type = mt.In(2)
			}
		}
	}
	return methods
}
func suitableHandlerMethods(typ reflect.Type, nameFunc func(string) string) map[string]*Handler {
	methods := make(map[string]*Handler)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mt := method.Type
		mn := method.Name
		if isHandlerMethod(method) {
			raw := false
			if mt.NumIn() == 3 && mt.In(2) == typeOfBytes {
				raw = true
			}
			// rewrite handler name
			if nameFunc != nil {
				mn = nameFunc(mn)
			}
			var msgType message.Type
			if mt.NumOut() == 0 {
				msgType = message.Notify
			} else {
				msgType = message.Request
			}
			handler := &Handler{
				Method:      method,
				IsRawArg:    raw,
				MessageType: msgType,
			}
			if mt.NumIn() == 3 {
				handler.Type = mt.In(2)
			}
			methods[mn] = handler
		}
	}
	return methods
}
