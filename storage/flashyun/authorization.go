package flashyun

import "context"

// Authorizer decides whether a subject may perform an action on a stable
// FlashYun resource object. The storage data plane never consults policy
// storage itself: it delegates the decision to the caller-supplied adapter,
// which forwards to Identity's authorization RPC.
type Authorizer interface {
	Authorize(ctx context.Context, subject, object, action string) (bool, error)
}

// AuthorizerFunc adapts a function to the Authorizer interface.
type AuthorizerFunc func(ctx context.Context, subject, object, action string) (bool, error)

// Authorize calls the wrapped function.
func (f AuthorizerFunc) Authorize(ctx context.Context, subject, object, action string) (bool, error) {
	return f(ctx, subject, object, action)
}
