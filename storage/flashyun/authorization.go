package flashyun

import (
	"context"
	"errors"

	"github.com/casbin/casbin/v3"
)

// Authorizer decides whether a subject may perform an action on a stable
// FlashYun resource object.
type Authorizer interface {
	Authorize(ctx context.Context, subject, object, action string) (bool, error)
}

// CasbinEnforcer is the portion of a Casbin enforcer used by this package.
type CasbinEnforcer interface {
	Enforce(rvals ...interface{}) (bool, error)
}

// CasbinAuthorizer adapts Casbin to the storage authorization boundary.
type CasbinAuthorizer struct {
	Enforcer CasbinEnforcer
}

var _ CasbinEnforcer = (*casbin.Enforcer)(nil)

// Authorize evaluates the configured Casbin policy.
func (a CasbinAuthorizer) Authorize(_ context.Context, subject, object, action string) (bool, error) {
	if a.Enforcer == nil {
		return false, errors.New("flashyun storage Casbin enforcer is required")
	}
	return a.Enforcer.Enforce(subject, object, action)
}
