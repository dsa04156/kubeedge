/*
Copyright 2026 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionCompletionRoundTrip(t *testing.T) {
	tests := []struct {
		status        ConnectionCompletionStatus
		completionErr error
	}{
		{status: ConnectionCompletionSuccess},
		{status: ConnectionCompletionError, completionErr: errors.New("scanner failed")},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			data, err := EncodeConnectionCompletion(tt.status, tt.completionErr)
			require.NoError(t, err)

			actual, err := DecodeConnectionCompletion(data)

			if tt.completionErr == nil {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrConnectionCompletionFailed)
				assert.ErrorContains(t, err, tt.completionErr.Error())
			}
			assert.Equal(t, tt.status, actual)
		})
	}
}

func TestDecodeConnectionCompletionRejectsUnknownPayload(t *testing.T) {
	for _, data := range [][]byte{
		nil,
		[]byte("not-json"),
		[]byte(`{"version":"v2","status":"success"}`),
		[]byte(`{"version":"v1","status":"maybe"}`),
		[]byte(`{"version":"v1","status":"error"}`),
	} {
		_, err := DecodeConnectionCompletion(data)

		assert.ErrorIs(t, err, ErrConnectionCompletionUnknown)
	}
}
