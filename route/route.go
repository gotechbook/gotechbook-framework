package route

import (
	"errors"
	"fmt"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"strings"
)

var (
	ErrRouteFieldCantEmpty = errors.New("route field can not be empty")
	ErrInvalidRoute        = errors.New("invalid route")
)

type Route struct {
	SvType  string
	Service string
	Method  string
}

func NewRoute(server, service, method string) *Route {
	return &Route{server, service, method}
}
func Decode(route string) (*Route, error) {
	r := strings.Split(route, ".")
	for _, s := range r {
		if strings.TrimSpace(s) == "" {
			return nil, ErrRouteFieldCantEmpty
		}
	}
	switch len(r) {
	case 3:
		return NewRoute(r[0], r[1], r[2]), nil
	case 2:
		return NewRoute("", r[0], r[1]), nil
	default:
		logger.Log.Errorf("invalid route: " + route)
		return nil, ErrInvalidRoute
	}
}

func (r *Route) String() string {
	if r.SvType != "" {
		return fmt.Sprintf("%s.%s.%s", r.SvType, r.Service, r.Method)
	}
	return r.Short()
}
func (r *Route) Short() string {
	return fmt.Sprintf("%s.%s", r.Service, r.Method)
}
