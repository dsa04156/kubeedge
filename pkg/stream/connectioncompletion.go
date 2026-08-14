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
	"encoding/json"
	"errors"
	"fmt"
)

const connectionCompletionVersion = "v1"

// ConnectionCompletionStatus describes how an edge-side stream ended.
type ConnectionCompletionStatus string

const (
	// ConnectionCompletionSuccess indicates that the edge read the complete response.
	ConnectionCompletionSuccess ConnectionCompletionStatus = "success"
	// ConnectionCompletionError indicates that the edge stream ended with an error.
	ConnectionCompletionError ConnectionCompletionStatus = "error"
)

var (
	// ErrConnectionCompletionUnknown indicates a legacy or invalid completion payload.
	ErrConnectionCompletionUnknown = errors.New("connection completion status is unknown")
	// ErrConnectionCompletionFailed indicates an explicit edge-side stream failure.
	ErrConnectionCompletionFailed = errors.New("edge connection completed with an error")
)

type connectionCompletion struct {
	Version string                     `json:"version"`
	Status  ConnectionCompletionStatus `json:"status"`
	Reason  string                     `json:"reason,omitempty"`
}

// EncodeConnectionCompletion creates a versioned completion payload.
func EncodeConnectionCompletion(status ConnectionCompletionStatus, completionErr error) ([]byte, error) {
	if status != ConnectionCompletionSuccess && status != ConnectionCompletionError {
		return nil, fmt.Errorf("unsupported connection completion status %q", status)
	}
	if status == ConnectionCompletionSuccess && completionErr != nil {
		return nil, errors.New("successful connection completion cannot contain an error")
	}
	if status == ConnectionCompletionError && completionErr == nil {
		return nil, errors.New("failed connection completion must contain an error")
	}

	reason := ""
	if completionErr != nil {
		reason = completionErr.Error()
	}
	return json.Marshal(connectionCompletion{
		Version: connectionCompletionVersion,
		Status:  status,
		Reason:  reason,
	})
}

// DecodeConnectionCompletion parses a versioned completion payload. Empty
// legacy payloads remain unknown so that failures from older peers are not
// interpreted as successful completion.
func DecodeConnectionCompletion(data []byte) (ConnectionCompletionStatus, error) {
	if len(data) == 0 {
		return "", ErrConnectionCompletionUnknown
	}

	var completion connectionCompletion
	if err := json.Unmarshal(data, &completion); err != nil {
		return "", fmt.Errorf("%w: %v", ErrConnectionCompletionUnknown, err)
	}
	if completion.Version != connectionCompletionVersion {
		return "", fmt.Errorf("%w: unsupported version %q", ErrConnectionCompletionUnknown, completion.Version)
	}
	if completion.Status != ConnectionCompletionSuccess && completion.Status != ConnectionCompletionError {
		return "", fmt.Errorf("%w: unsupported status %q", ErrConnectionCompletionUnknown, completion.Status)
	}
	if completion.Status == ConnectionCompletionError {
		if completion.Reason == "" {
			return "", fmt.Errorf("%w: error completion has no reason", ErrConnectionCompletionUnknown)
		}
		return completion.Status, fmt.Errorf("%w: %s", ErrConnectionCompletionFailed, completion.Reason)
	}
	return completion.Status, nil
}
