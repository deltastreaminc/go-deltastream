/*
Copyright (c) 2024, DeltaStream Inc.

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
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/utils/ptr"

	"github.com/deltastreaminc/go-deltastream/apiv2"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver        = Driver{}     // original go interface
	_ driver.DriverContext = Driver{}     // latest go interface
	_ driver.Connector     = &connector{} // latest go interface
)

func init() {
	sql.Register("deltastream", &Driver{})
}

// Driver is the DeltaStream database driver.
type Driver struct{}

type connector struct {
	connStr string
}

// OpenConnector parses the connection string and returns a new connector.
func (Driver) OpenConnector(connStr string) (driver.Connector, error) {
	return &connector{connStr: connStr}, nil
}

// Open returns a new connection to the database. (sql.DB compatibility)
func (Driver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

// Open returns a new connection to the database. (sql.DB compatibility)
func Open(connStr string) (driver.Conn, error) {
	return OpenWithHTTPClient(context.TODO(), &http.Client{}, connStr)
}

// OpenWithHTTPClient returns a new connection to the database. The returned connection must only used by one goroutine at a time.
func OpenWithHTTPClient(ctx context.Context, httpClient *http.Client, connStr string) (driver.Conn, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid uri: %w", err)
	}
	params := u.Query()
	u.RawQuery = ""

	token := params.Get("token")
	if token == "" {
		return nil, &ErrClientError{message: "no api token provided"}
	}
	var sessionID *string
	if params.Has("sessionID") {
		sessionID = ptr.To(params.Get("sessionID"))
	}

	client, err := apiv2.NewClientWithResponses(
		u.String(),
		apiv2.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Add("Authorization", "Bearer "+token)
			return nil
		}),
		apiv2.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize client: %w", err)
	}

	return &conn{
		client:    client,
		rsctx:     &apiv2.ResultSetContext{},
		sessionID: sessionID,
	}, nil
}

// Connect returns a connection to the database. The returned connection must only used by one goroutine at a time.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return OpenWithHTTPClient(ctx, &http.Client{}, c.connStr)
}

// Driver returns the underlying Driver of the Connector for backward compatibility with sql.DB.
func (*connector) Driver() driver.Driver {
	return Driver{}
}
