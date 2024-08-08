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
	"context"
	"database/sql"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/deltastreaminc/go-deltastream/apiv2"
	"github.com/deltastreaminc/go-deltastream/dpapiv2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"k8s.io/utils/ptr"
)

type DPConn struct {
	apiv2.DataplaneRequest
	client    *dpapiv2.ClientWithResponses
	sessionID *string
}

func NewDPConn(dpreq apiv2.DataplaneRequest, sessionID *string, httpClient *http.Client) (*DPConn, error) {
	uri, err := url.Parse(dpreq.Uri)
	if err != nil {
		return nil, &ErrInterfaceError{message: "invalid dataplane uri"}
	}
	uri.Path = "/v2"

	client, err := dpapiv2.NewClientWithResponses(
		uri.String(),
		dpapiv2.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Add("Authorization", "Bearer "+dpreq.Token)
			if os.Getenv("DELTASTREAM_MAINTENANCE") != "" {
				// Allows requests to be made during maintenance. This is only used for test
				req.Header.Add("deltastream-maintenance", "yes")
			}
			return nil
		}),
		dpapiv2.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, err
	}
	return &DPConn{
		client:           client,
		DataplaneRequest: dpreq,
		sessionID:        sessionID,
	}, nil
}

func (c *DPConn) getStatement(ctx context.Context, statementID uuid.UUID, partitionID int32) (rs *apiv2.ResultSet, err error) {
	if c.client == nil {
		return nil, sql.ErrConnDone
	}

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		resp, err := c.client.GetStatementStatusWithResponse(ctx, statementID, &dpapiv2.GetStatementStatusParams{PartitionID: &partitionID, SessionID: c.sessionID, Timezone: ptr.To("UTC")})
		if err != nil {
			return nil, &ErrInterfaceError{wrapErr: err, message: "unable to send request to server"}
		}
		switch {
		case resp.JSON200 != nil:
			if resp.JSON200.SqlState == string(SqlStateSuccessfulCompletion) {
				return resp.JSON200, nil
			}
			return nil, ErrSQLError{
				SQLCode:     SqlState(resp.JSON200.SqlState),
				Message:     ptr.Deref(resp.JSON200.Message, ""),
				StatementID: resp.JSON200.StatementID,
			}
		case resp.JSON202 != nil:
			continue
		case resp.JSON400 != nil:
			return nil, &ErrInterfaceError{message: resp.JSON400.Message}
		case resp.JSON403 != nil:
			return nil, errors.Errorf(resp.JSON403.Message+": %w", ErrAuthenticationError)
		case resp.JSON404 != nil:
			return nil, &ErrInterfaceError{message: resp.JSON404.Message}
		case resp.JSON408 != nil:
			return nil, errors.Errorf(resp.JSON408.Message+": %w", ErrDeadlineExceeded)
		case resp.JSON500 != nil:
			return nil, &ErrServerError{message: resp.JSON500.Message}
		case resp.JSON503 != nil:
			return nil, errors.Errorf(resp.JSON500.Message+": %w", ErrServiceUnavailable)
		default:
			return nil, &ErrServerError{message: "unexpected response"}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			continue
		}
	}
}
