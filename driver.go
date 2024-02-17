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
	"crypto/tls"
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
	client *apiv2.ClientWithResponses
	opts   connectionOptions
}

// OpenConnector parses the connection string and returns a new connector.
func (Driver) OpenConnector(connStr string) (driver.Connector, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid uri: %w", err)
	}
	params := u.Query()
	u.RawQuery = ""

	options := []ConnectionOption{WithServer(u.String())}
	var token string
	if u := u.User; u != nil {
		if t, ok := u.Password(); ok {
			options = append(options, WithStaticToken(t))
			token = t
		}
	}
	if token == "" {
		return nil, &ErrClientError{message: "no api token provided"}
	}

	if params.Has("sessionID") {
		options = append(options, WithSessionID(params.Get("sessionID")))
	}

	ctx := context.Background()
	connector, err := ConnectorWithOptions(ctx, options...)
	if err != nil {
		return nil, err
	}
	return connector, nil
}

// Open returns a new connection to the database. (sql.DB compatibility)
func (Driver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

// Open returns a new connection to the database. (sql.DB compatibility)
func Open(connStr string) (driver.Conn, error) {
	connector, err := (Driver{}).OpenConnector(connStr)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

type connectionOptions struct {
	staticToken *string
	sessionID   *string
	server      string
	insecureTLS bool
	httpClient  *http.Client
	authClient  AuthClient
}

func WithStaticToken(token string) func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.staticToken = ptr.To(token)
	}
}

func WithInsecureTLS() func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.insecureTLS = true
	}
}

func WithAuthClient(authClient AuthClient) func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.authClient = authClient
	}
}

func WithHTTPClient(client *http.Client) func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.httpClient = client
	}
}

func WithSessionID(sessionID string) func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.sessionID = ptr.To(sessionID)
	}
}

func WithServer(server string) func(*connectionOptions) {
	return func(o *connectionOptions) {
		o.server = server
	}
}

type ConnectionOption func(*connectionOptions)

// OpenWithHTTPClient returns a new connection to the database. The returned connection must only used by one goroutine at a time.
func ConnectorWithOptions(ctx context.Context, options ...ConnectionOption) (*connector, error) {
	opts := connectionOptions{
		httpClient: http.DefaultClient,
		server:     "https://api.deltastream.com/v2",
	}
	for _, o := range options {
		o(&opts)
	}

	var tokenManager TokenManager
	if opts.authClient != nil {
		tokenManager = NewTokenManager(ctx, opts.authClient)
	}
	if opts.staticToken != nil {
		tokenManager = NewStaticTokenManager(ctx, *opts.staticToken)
	}
	if tokenManager == nil {
		return nil, &ErrClientError{message: "no api token provided"}
	}
	if opts.insecureTLS {
		if opts.httpClient.Transport == nil {
			opts.httpClient.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
		}
	}

	u, err := url.Parse(opts.server)
	if err != nil {
		return nil, &ErrClientError{message: "invalid server url", wrapErr: err}
	}
	opts.server = fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.Path)

	client, err := apiv2.NewClientWithResponses(
		opts.server,
		apiv2.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			token, err := tokenManager.GetToken(ctx)
			if err != nil {
				return err
			}
			req.Header.Add("Authorization", "Bearer "+token)
			return nil
		}),
		apiv2.WithHTTPClient(opts.httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize client: %w", err)
	}

	return &connector{
		client: client,
		opts:   opts,
	}, nil
}

// Connect returns a connection to the database. The returned connection must only used by one goroutine at a time.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{
		client:     c.client,
		rsctx:      &apiv2.ResultSetContext{},
		sessionID:  c.opts.sessionID,
		httpClient: c.opts.httpClient,
	}, nil
}

// Driver returns the underlying Driver of the Connector for backward compatibility with sql.DB.
func (*connector) Driver() driver.Driver {
	return Driver{}
}
