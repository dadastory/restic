package resticstore

import (
	"context"
	"errors"
	"net"

	"github.com/rclone/rclone/fs/fserrors"
)

// OperationError retains only redacted text and bounded failure classification.
// It deliberately does not unwrap the original backend error: SDK errors may
// carry signed URLs, credential-bearing provider options or object locators.
type OperationError struct {
	message            string
	backendUnavailable bool
	canceled           bool
	deadline           bool
}

func (failure *OperationError) Error() string            { return failure.message }
func (failure *OperationError) BackendUnavailable() bool { return failure.backendUnavailable }
func (failure *OperationError) Is(target error) bool {
	return failure.canceled && target == context.Canceled || failure.deadline && target == context.DeadlineExceeded
}

func newOperationError(message string, cause error) error {
	return &OperationError{message: message, backendUnavailable: backendUnavailable(cause),
		canceled: errors.Is(cause, context.Canceled), deadline: errors.Is(cause, context.DeadlineExceeded)}
}

func backendUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var classified interface{ BackendUnavailable() bool }
	if errors.As(err, &classified) {
		return classified.BackendUnavailable()
	}
	var status interface{ HTTPStatusCode() int }
	if errors.As(err, &status) {
		code := status.HTTPStatusCode()
		return code >= 500 || code == 429 || code == 401 || code == 403
	}
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		switch coded.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "NotFound", "NoSuchUpload":
			return false
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "SlowDown", "Throttling", "ThrottlingException", "RequestTimeout":
			return true
		}
	}
	var network net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network) || fserrors.ShouldRetry(err) || fserrors.IsRetryError(err)
}
