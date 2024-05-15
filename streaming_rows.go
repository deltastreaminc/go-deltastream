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
	"crypto/tls"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/deltastreaminc/go-deltastream/apiv2"
	"github.com/gorilla/websocket"
	"k8s.io/utils/ptr"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Rows                           = &streamingRows{}
	_ driver.RowsColumnTypeScanType         = &streamingRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &streamingRows{}
	_ driver.RowsColumnTypeNullable         = &streamingRows{}
	_ driver.RowsColumnTypeLength           = &streamingRows{}
	_ driver.RowsColumnTypePrecisionScale   = &streamingRows{}
)

type streamingRows struct {
	conn *websocket.Conn

	metadata  *PrintTopicMetadataMessage
	readyChan chan struct{}
	dataChan  chan *PrintTopicDataMessage
	errChan   chan error
}

type AuthMessage struct {
	Type        string `json:"type"`
	AccessToken string `json:"accessToken"`
	SessionID   string `json:"sessionId"`
}

type PrintTopicMessage struct {
	Type     string                    `json:"type"`
	Err      PrintTopicErrorMessage    `json:"-"`
	Metadata PrintTopicMetadataMessage `json:"-"`
	Data     PrintTopicDataMessage     `json:"-"`
}

func (m *PrintTopicMessage) UnmarshalJSON(b []byte) error {
	var t struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return err
	}
	m.Type = t.Type
	switch t.Type {
	case "error":
		if err := json.Unmarshal(b, &m.Err); err != nil {
			return err
		}
	case "data":
		if err := json.Unmarshal(b, &m.Data); err != nil {
			return err
		}
	case "metadata":
		if err := json.Unmarshal(b, &m.Metadata); err != nil {
			return err
		}
	default:
		return &ErrInterfaceError{message: "unexpected message type"}
	}
	return nil
}

type PrintTopicErrorMessage struct {
	Type    string            `json:"type"`
	Headers map[string]string `json:"headers"`
	Message string            `json:"message"`
	SqlCode SqlState          `json:"sqlCode"`
}

type PrintTopicColumn struct {
	Name      string `json:"name"`
	Nullable  bool   `json:"nullable,omitempty"`
	Type      string `json:"type"`
	Length    int64  `json:"length,omitempty"`
	Precision int64  `json:"precision,omitempty"`
	Scale     int64  `json:"scale,omitempty"`
}

type PrintTopicMetadataMessage struct {
	Type    string             `json:"type"`
	Headers map[string]string  `json:"headers"`
	Columns []PrintTopicColumn `json:"columns"`
}

type PrintTopicDataMessage struct {
	Type    string            `json:"type"`
	Headers map[string]string `json:"headers"`
	Data    []*string         `json:"data"`
}

func newStreamingRows(ctx context.Context, req apiv2.DataplaneRequest, httpClient *http.Client, sessionID *string) (*streamingRows, error) {
	u, err := url.Parse(req.Uri)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, &ErrInterfaceError{message: "unsupported scheme in streaming result set"}
	}

	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
	if t, ok := httpClient.Transport.(*http.Transport); ok {
		dialer.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: t.TLSClientConfig.InsecureSkipVerify,
		}
	}
	h := http.Header{}
	if sessionID != nil {
		h.Add("ds-session-id", *sessionID)
	}

	conn, resp, err := dialer.DialContext(ctx, u.String(), h)
	if err != nil {
		if resp != nil && resp.StatusCode != 200 {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, &ErrClientError{message: "unable to read dataplane response", wrapErr: err}
			}
			return nil, &ErrInterfaceError{message: string(b)}
		}
		return nil, err
	}

	if err = conn.WriteJSON(&AuthMessage{
		Type:        "auth",
		AccessToken: req.Token,
		SessionID:   ptr.Deref(sessionID, ""),
	}); err != nil {
		return nil, &ErrInterfaceError{message: "unable to send request", wrapErr: err}
	}

	rows := &streamingRows{
		conn:      conn,
		dataChan:  make(chan *PrintTopicDataMessage, 30),
		readyChan: make(chan struct{}),
		errChan:   make(chan error),
	}
	go rows.readMessages()
	select {
	case <-rows.readyChan:
	case err = <-rows.errChan:
		return nil, err
	}

	return rows, nil
}

func (r *streamingRows) readMessages() {
	defer close(r.readyChan)

	r.conn.SetReadDeadline(time.Time{})
	for {
		var msg PrintTopicMessage
		if err := r.conn.ReadJSON(&msg); err != nil {
			r.errChan <- &ErrInterfaceError{message: "unable to read message from server", wrapErr: err}
			return
		}
		switch msg.Type {
		case "error":
			r.errChan <- &ErrSQLError{SQLCode: msg.Err.SqlCode, Message: msg.Err.Message}
			return
		case "metadata":
			r.metadata = &msg.Metadata
			r.readyChan <- struct{}{}
		case "data":
			r.dataChan <- &msg.Data
		default:
			r.errChan <- &ErrInterfaceError{message: "unexpected message type " + msg.Type}
			return
		}
	}
}

func (r *streamingRows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	if r.metadata == nil {
		return false, false
	}
	if index < 0 || index >= len(r.metadata.Columns) {
		return false, false
	}
	return r.metadata.Columns[index].Nullable, true
}

func (r *streamingRows) ColumnTypeDatabaseTypeName(index int) string {
	if r.metadata == nil {
		return ""
	}
	if index < 0 || index >= len(r.metadata.Columns) {
		return ""
	}
	return r.metadata.Columns[index].Name
}

func (r *streamingRows) ColumnTypeScanType(index int) reflect.Type {
	if r.metadata == nil {
		return nil
	}
	if index < 0 || index >= len(r.metadata.Columns) {
		return nil
	}
	md := r.metadata.Columns[index]
	switch {
	case strings.HasPrefix(md.Type, "VARCHAR"):
		return typeMap["VARCHAR"]
	case strings.HasPrefix(md.Type, "DECIMAL"):
		return typeMap["DECIMAL"]
	case strings.HasPrefix(md.Type, "TIMESTAMP"):
		return typeMap["TIMESTAMP"]
	case strings.HasPrefix(md.Type, "TIME"):
		return typeMap["TIME"]
	case strings.HasPrefix(md.Type, "ARRAY"):
		return typeMap["ARRAY"]
	case strings.HasPrefix(md.Type, "STRUCT"):
		return typeMap["STRUCT"]
	case strings.HasPrefix(md.Type, "MAP"):
		return typeMap["MAP"]
	default:
		return typeMap[md.Type]
	}
}

func (r *streamingRows) Close() error {
	r.metadata = nil
	close(r.dataChan)
	err := r.conn.Close()
	if err != nil {
		return &ErrInterfaceError{message: "error while closing connection", wrapErr: err}
	}
	return nil
}

func (r *streamingRows) Columns() []string {
	if r.metadata == nil {
		return nil
	}
	ret := make([]string, len(r.metadata.Columns))
	for i, c := range r.metadata.Columns {
		ret[i] = c.Name
	}
	return ret
}

func (r *streamingRows) ColumnTypePrecisionScale(index int) (precision int64, scale int64, ok bool) {
	if r.metadata == nil {
		return 0, 0, false
	}
	if index < 0 || index >= len(r.metadata.Columns) {
		return 0, 0, false
	}
	md := r.metadata.Columns[index]
	return md.Precision, md.Scale, true
}

func (r *streamingRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if r.metadata == nil {
		return 0, false
	}
	if index < 0 || index >= len(r.metadata.Columns) {
		return 0, false
	}
	md := r.metadata.Columns[index]
	return md.Length, true
}

// Next implements driver.Rows.
func (r *streamingRows) Next(dest []driver.Value) error {
	var rowData *PrintTopicDataMessage
	var open bool
	var err error

	select {
	case rowData, open = <-r.dataChan:
		if !open {
			return io.EOF
		}
	case err = <-r.errChan:
		return err
	}

	if len(rowData.Data) != len(dest) {
		return &ErrClientError{message: fmt.Sprintf("number of columns does not match size of result slice. expected %d, got %d", len(rowData.Data), len(dest))}
	}

	for idx, col := range r.metadata.Columns {
		switch {
		case rowData.Data[idx] == nil:
			dest[idx] = nil
		default:
			fallthrough
		case strings.HasPrefix(col.Type, "VARCHAR") || strings.HasPrefix(col.Type, "ARRAY") || strings.HasPrefix(col.Type, "MAP") || strings.HasPrefix(col.Type, "STRUCT"):
			dest[idx] = *rowData.Data[idx]
		case col.Type == "TINYINT" || col.Type == "SMALLINT" || col.Type == "INTEGER" || col.Type == "BIGINT":
			dest[idx], err = strconv.ParseInt(*rowData.Data[idx], 10, 64)
			if err != nil {
				return err
			}
		case col.Type == "FLOAT" || col.Type == "DOUBLE" || strings.HasPrefix(col.Type, "DECIMAL"):
			dest[idx], err = strconv.ParseFloat(*rowData.Data[idx], 64)
			if err != nil {
				return err
			}
		case strings.HasPrefix(col.Type, "TIME") || col.Type == "DATE":
			dest[idx], err = parseTime(*rowData.Data[idx], col.Type)
			if err != nil {
				return err
			}
		case col.Type == "VARBINARY" || col.Type == "BYTES":
			dest[idx], err = base64.StdEncoding.DecodeString(*rowData.Data[idx])
			if err != nil {
				return err
			}
		case col.Type == "BOOLEAN":
			dest[idx] = strings.ToLower(*rowData.Data[idx]) == "true"
		}
	}
	return nil
}
