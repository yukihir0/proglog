package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enfocer *casbin.Enforcer
}

func New(model, policy string) *Authorizer {
	enfocer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enfocer: enfocer,
	}
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enfocer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)

		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
