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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"k8s.io/utils/ptr"

	"github.com/deltastreaminc/go-deltastream/apiv2"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Conn           = &conn{} // Conn is a connection to a database. Stateful and not multi-goroutine safe.
	_ driver.Pinger         = &conn{} // Check DB connection. Used for pooling. Returns ErrBadConn if in bad state.
	_ driver.Execer         = &conn{} // Provide exec function on conn without having to prepare a statement
	_ driver.ExecerContext  = &conn{} // ditto with context
	_ driver.Queryer        = &conn{} // Provide query function on conn without having to prepare a statement
	_ driver.QueryerContext = &conn{} // ditto with context
)

type conn struct {
	client    *apiv2.ClientWithResponses
	rsctx     *apiv2.ResultSetContext
	sessionID *string
	sync.RWMutex
}

// region driver.Conn

func (*conn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	c.client = nil
	return nil
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.client == nil {
		return nil, driver.ErrBadConn
	}

	return &statement{
		c:     c,
		query: query,
	}, nil
}

// endregion

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.TODO(), query, convertArgs(args))
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.TODO(), query, convertArgs(args))
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c == nil {
		return nil, driver.ErrBadConn
	}

	var attchments map[string]io.ReadCloser
	if v := ctx.Value(sqlRequestAttachmentsKey); v != nil {
		if v, ok := v.(*sqlRequestAttachments); ok {
			attchments = v.attachments
		}
	}

	_, err := c.submitStatement(ctx, attchments, query)
	if err != nil {
		return nil, err
	}

	return &result{}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c == nil {
		return nil, driver.ErrBadConn
	}

	var attchments map[string]io.ReadCloser
	if v := ctx.Value(sqlRequestAttachmentsKey); v != nil {
		if v, ok := v.(*sqlRequestAttachments); ok {
			attchments = v.attachments
		}
	}

	rs, err := c.submitStatement(ctx, attchments, query)
	if err != nil {
		return nil, err
	}

	return &rows{ctx: ctx, conn: c, currentRowIdx: -1, currentPartitionIdx: 0, currentResultSet: rs}, nil

}

func (c *conn) Ping(ctx context.Context) error {
	resp, err := c.client.GetVersion(ctx)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) setResultSetContext(rsctx *apiv2.ResultSetContext) {
	c.Lock()
	defer c.Unlock()
	c.rsctx = rsctx
}

func (c *conn) getResultSetContext() (rsctx *apiv2.ResultSetContext) {
	c.RLock()
	defer c.RUnlock()
	return c.rsctx
}

func (c *conn) submitStatement(ctx context.Context, attachments map[string]io.ReadCloser, query string) (rs *apiv2.ResultSet, err error) {
	if c.client == nil {
		return nil, sql.ErrConnDone
	}

	rsctx := c.getResultSetContext()

	request := &apiv2.SubmitStatementJSONRequestBody{
		Statement: query,
		Role:      rsctx.RoleName,
		Database:  rsctx.DatabaseName,
		Schema:    rsctx.SchemaName,
		Store:     rsctx.StoreName,
	}
	if rsctx.OrganizationID != nil {
		request.Organization = ptr.To(rsctx.OrganizationID.String())
	}

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)

	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="request";`)
	h.Set("Content-Type", "application/json")
	part, err := writer.CreatePart(h)
	if err != nil {
		return nil, &ErrClientError{message: "error building request", wrapErr: err}
	}
	if err = json.NewEncoder(part).Encode(request); err != nil {
		return nil, &ErrClientError{message: "error building request", wrapErr: err}
	}

	for k, f := range attachments {
		w, err := writer.CreateFormFile("attachment", k)
		if err != nil {
			return nil, &ErrClientError{message: "error building request", wrapErr: err}
		}
		_, err = io.Copy(w, f)
		if err != nil {
			return nil, &ErrClientError{message: "error building request", wrapErr: err}
		}
	}

	writer.Close()

	resp, err := c.client.SubmitStatementWithBodyWithResponse(ctx, writer.FormDataContentType(), body)
	if err != nil {
		return nil, &ErrInterfaceError{wrapErr: err, message: "unable to send request to server"}
	}
	switch {
	case resp.JSON200 != nil:
		if resp.JSON200.SqlState == string(SqlStateSuccessfulCompletion) {
			c.setResultSetContext(resp.JSON200.Metadata.Context)
			return resp.JSON200, nil
		}
		return nil, ErrSQLError{
			SQLCode:     SqlState(resp.JSON200.SqlState),
			Message:     ptr.Deref(resp.JSON200.Message, ""),
			StatementID: resp.JSON200.StatementID,
		}
	case resp.JSON202 != nil:
		return c.getStatement(ctx, resp.JSON202.StatementID, 0)
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
		return nil, &ErrInterfaceError{message: fmt.Sprintf("unexpected response from server. status code: %d", resp.HTTPResponse.StatusCode)}
	}
}

func (c *conn) getStatement(ctx context.Context, statementID uuid.UUID, partitionID int32) (rs *apiv2.ResultSet, err error) {
	if c.client == nil {
		return nil, sql.ErrConnDone
	}

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		resp, err := c.client.GetStatementStatusWithResponse(ctx, statementID, &apiv2.GetStatementStatusParams{PartitionID: &partitionID, SessionID: c.sessionID, Timezone: ptr.To("UTC")})
		if err != nil {
			return nil, &ErrInterfaceError{wrapErr: err, message: "unable to send request to server"}
		}
		switch {
		case resp.JSON200 != nil:
			if resp.JSON200.SqlState == string(SqlStateSuccessfulCompletion) {
				c.setResultSetContext(resp.JSON200.Metadata.Context)
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
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			continue
		}
	}
}

func convertArgs(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for idx := range args {
		out[idx] = driver.NamedValue{Ordinal: idx + 1, Value: args[idx]}
	}
	return out
}
