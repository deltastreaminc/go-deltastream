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
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"

	"github.com/deltastreaminc/go-deltastream/apiv2"
)

func mockSubmitStatementsResponser(g *gomega.WithT, statusCode int, expectedToken, expectedSQL string, expectedAttachments map[string][]byte, fixture string) func(r *http.Request) (*http.Response, error) {
	return func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		g.Expect(err).To(BeNil())
		g.Expect(mediaType).Should(ContainSubstring("multipart/"))

		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			switch p.FormName() {
			case "request":
				g.Expect(p.Header.Get("Content-Type")).To(Equal("application/json"))
				req := &apiv2.SubmitStatementJSONRequestBody{}
				err = json.NewDecoder(p).Decode(req)
				g.Expect(err).To(BeNil())
				g.Expect(req.Statement).To(Equal(expectedSQL))
			case "attachments":
				name := p.FileName()
				data, err := io.ReadAll(p)
				g.Expect(err).To(BeNil())
				attachment, ok := expectedAttachments[name]
				g.Expect(ok).To(BeTrue())
				g.Expect(attachment).To(Equal(data))
				delete(expectedAttachments, name)
			}
		}
		g.Expect(expectedAttachments).To(BeEmpty())

		f, err := os.OpenFile(fixture, os.O_RDONLY, 0600)
		g.Expect(err).To(BeNil())
		h := http.Header{}
		h.Add("Content-Type", "application/json")
		return &http.Response{StatusCode: statusCode, Body: f, Header: h}, nil
	}
}

func mockGetStatementResponser(g *gomega.WithT, statusCode int, expectedToken, fixture string) func(r *http.Request) (*http.Response, error) {
	return func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer "+expectedToken {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}

		f, err := os.OpenFile(fixture, os.O_RDONLY, 0600)
		g.Expect(err).To(BeNil())
		h := http.Header{}
		h.Add("Content-Type", "application/json")
		return &http.Response{StatusCode: statusCode, Body: f, Header: h}, nil
	}
}

type ColumnMatcher struct {
	Name         string
	DatabaseType string
}

// FailureMessage implements types.GomegaMatcher.
func (c *ColumnMatcher) FailureMessage(actual interface{}) (message string) {
	col, ok := actual.(*sql.ColumnType)
	if !ok {
		return "expected sql.ColumnType"
	}

	if col.DatabaseTypeName() != c.DatabaseType {
		return "expected database type " + c.DatabaseType + " but got " + col.DatabaseTypeName()
	}

	if col.Name() != c.Name {
		return "expected column name " + c.Name + " but got " + col.Name()
	}
	return ""
}

// Match implements types.GomegaMatcher.
func (c *ColumnMatcher) Match(actual interface{}) (success bool, err error) {
	errMsg := c.FailureMessage(actual)
	if errMsg == "" {
		return true, nil
	}
	return false, fmt.Errorf(errMsg)
}

// NegatedFailureMessage implements types.GomegaMatcher.
func (c *ColumnMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	panic("unimplemented")
}

var _ types.GomegaMatcher = &ColumnMatcher{}

func TestSimpleResultset(t *testing.T) {
	g := gomega.NewWithT(t)
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/version", func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{ "major": 1, "minor": 0, "patch": 0 }`))}, nil
	})

	httpmock.RegisterResponder("POST", "https://api.deltastream.io/v2/statements",
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "LIST ORGANIZATIONS;", map[string][]byte{}, "fixtures/list-organizations-200-00000-1.json"),
	)

	db, err := sql.Open("deltastream", "https://_:sometoken@api.deltastream.io/v2")
	g.Expect(err).To(BeNil())

	rows, err := db.Query("LIST ORGANIZATIONS;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.Columns()).To(Equal([]string{"id", "name", "description", "profileImageURI", "createdAt"}))

	g.Expect(rows.ColumnTypes()).To(ContainElements(
		&ColumnMatcher{Name: "id", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "name", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "description", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "profileImageURI", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "createdAt", DatabaseType: "TIMESTAMP_TZ"},
	))

	var (
		id              string
		name            string
		description     *string
		profileImageURI *string
		createdAt       time.Time
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &description, &profileImageURI, &createdAt)
		g.Expect(err).To(BeNil())
	}
	g.Expect(rows.Err()).To(BeNil())

	tm, _ := time.Parse(time.RFC3339, "2023-12-30T03:37:45Z")
	var nilstr *string
	g.Expect([]any{id, name, description, profileImageURI, createdAt}).To(Equal([]any{"0e0e3617-3cd6-4407-a189-97daf226c4d4", "o1", nilstr, nilstr, tm}))
}

func TestSimpleResultsetWithDisplayHints(t *testing.T) {
	g := gomega.NewWithT(t)
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/version", func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{ "major": 1, "minor": 0, "patch": 0 }`))}, nil
	})

	httpmock.RegisterResponder("POST", "https://api.deltastream.io/v2/statements",
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "LIST ORGANIZATIONS;", map[string][]byte{}, "fixtures/list-organizations-200-00000-1.json"),
	)

	server := "https://api.deltastream.io/v2"
	connOptions := []ConnectionOption{WithColumnDisplayHints(), WithServer(server), WithStaticToken("sometoken")}
	connector, err := ConnectorWithOptions(context.TODO(), connOptions...)
	g.Expect(err).To(BeNil())
	db := sql.OpenDB(connector)

	rows, err := db.Query("LIST ORGANIZATIONS;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.ColumnTypes()).To(ContainElements(
		&ColumnMatcher{Name: "id", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "name", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "description", DatabaseType: "VARCHAR"},
		&ColumnMatcher{Name: "profileImageURI", DatabaseType: "VARCHAR;nowrap"},
		&ColumnMatcher{Name: "createdAt", DatabaseType: "TIMESTAMP_TZ"},
	))
}

func TestEmptyResultset(t *testing.T) {
	g := gomega.NewWithT(t)
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/version", func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{ "major": 1, "minor": 0, "patch": 0 }`))}, nil
	})

	httpmock.RegisterResponder("POST", "https://api.deltastream.io/v2/statements",
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "LIST ORGANIZATIONS;", map[string][]byte{}, "fixtures/list-organizations-200-00000-0.json"),
	)
	db, err := sql.Open("deltastream", "https://_:sometoken@api.deltastream.io/v2")
	g.Expect(err).To(BeNil())

	rows, err := db.Query("LIST ORGANIZATIONS;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.Columns()).To(Equal([]string{"id", "name", "description", "profileImageURI", "createdAt"}))
	g.Expect(rows.Next()).To(BeFalse())
	g.Expect(rows.Err()).To(BeNil())
}

func TestDelayedResultset(t *testing.T) {
	g := gomega.NewWithT(t)
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/version", func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{ "major": 1, "minor": 0, "patch": 0 }`))}, nil
	})

	count := 0
	httpmock.RegisterResponder("POST", "https://api.deltastream.io/v2/statements", mockSubmitStatementsResponser(g, http.StatusAccepted, "sometoken", "LIST ORGANIZATIONS;", map[string][]byte{}, "fixtures/list-organizations-202-03000.json"))
	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/statements/d789687d-4e1b-4649-846e-4f10b722f3ad?partitionID=0&timezone=UTC", func(r *http.Request) (*http.Response, error) {
		if count < 3 {
			count = count + 1
			return mockGetStatementResponser(g, http.StatusAccepted, "sometoken", "fixtures/list-organizations-202-03000.json")(r)
		}
		return mockGetStatementResponser(g, http.StatusOK, "sometoken", "fixtures/list-organizations-200-00000-1.json")(r)
	})

	db, err := sql.Open("deltastream", "https://_:sometoken@api.deltastream.io/v2")
	g.Expect(err).To(BeNil())

	rows, err := db.Query("LIST ORGANIZATIONS;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.Columns()).To(Equal([]string{"id", "name", "description", "profileImageURI", "createdAt"}))

	var (
		id              string
		name            string
		description     *string
		profileImageURI *string
		createdAt       time.Time
	)
	for rows.Next() {
		err = rows.Scan(&id, &name, &description, &profileImageURI, &createdAt)
		g.Expect(err).To(BeNil())
	}
	g.Expect(rows.Err()).To(BeNil())

	tm, _ := time.Parse(time.RFC3339, "2023-12-30T03:37:45Z")
	var nilstr *string
	g.Expect([]any{id, name, description, profileImageURI, createdAt}).To(Equal([]any{"0e0e3617-3cd6-4407-a189-97daf226c4d4", "o1", nilstr, nilstr, tm}))
}
