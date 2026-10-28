package resticstore

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationErrorPreservesSafeTransportClassification(t *testing.T) {
	store := newSecretStore(t, "source-access", "source-secret", "source-password")
	raw := &url.Error{Op: "Get", URL: "https://source-secret.example/private", Err: context.DeadlineExceeded}
	err := operationErrorForStores("read payload", raw, store)
	classification, ok := err.(interface{ BackendUnavailable() bool })
	require.True(t, ok, "the redaction boundary must retain bounded backend fault classification")
	require.True(t, classification.BackendUnavailable())
	require.NotContains(t, err.Error(), "source-secret")
	require.Nil(t, errors.Unwrap(err), "raw credential-bearing causes must not escape redaction")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	canceled := operationErrorForStores("append", context.Canceled, store)
	require.ErrorIs(t, canceled, context.Canceled)
	require.False(t, canceled.(interface{ BackendUnavailable() bool }).BackendUnavailable())
	unknown := operationErrorForStores("commit", errors.New(strings.Repeat("source-password", 2)), store)
	require.False(t, unknown.(interface{ BackendUnavailable() bool }).BackendUnavailable())
}
