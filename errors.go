/*
Copyright (c) 2024-present, DeltaStream Inc.

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

package godeltastream

import (
	"fmt"

	"github.com/google/uuid"
)

var ErrNotSupported = &ErrClientError{message: "feature is not supported"}
var ErrAuthenticationError = fmt.Errorf("error while authenticating with server")
var ErrDeadlineExceeded = fmt.Errorf("deadline exceeded")
var ErrServiceUnavailable = fmt.Errorf("service temporarily unavailable")

type ErrStatementClosed struct{}

func (*ErrStatementClosed) Error() string { return "statement is closed" }

// ErrInterfaceError is raised when there is a mismatch between the expected interface between client and server
type ErrInterfaceError struct {
	message string
	wrapErr error
}

func (e *ErrInterfaceError) Error() string {
	if e.message == "" {
		return "connection is closed"
	} else if e.wrapErr != nil {
		return e.wrapErr.Error()
	}
	return e.message
}

func (e *ErrInterfaceError) Unwrap() error {
	return e.wrapErr
}

// ErrServerError is raised when server has an internal error while processing a message
type ErrServerError struct {
	message string
}

func (e *ErrServerError) Error() string {
	return e.message
}

// ErrClientError is raised when client has an internal error while processing a message
type ErrClientError struct {
	message string
	wrapErr error
}

func (e *ErrClientError) Error() string {
	if e.message == "" {
		return "connection is closed"
	} else if e.wrapErr != nil {
		return e.wrapErr.Error()
	}
	return e.message
}

func (e *ErrClientError) Unwrap() error {
	return e.wrapErr
}

type ErrSQLError struct {
	SQLCode     SqlState
	Message     string
	StatementID uuid.UUID
}

func (e ErrSQLError) Error() string {
	return e.Message
}
