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
	"io"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
)

func TestDPConn_Query(t *testing.T) {
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
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "SELECT * FROM mview_table;", map[string][]byte{}, "fixtures/dataplane-query-200-00000-0.json"),
	)

	httpmock.RegisterResponder("GET", "https://dpapi.deltastream.io/v2/statements/d789687d-4e1b-4649-846e-4f10b722f3ad?partitionID=0&timezone=UTC",
		mockGetStatementResponser(g, http.StatusOK, "dataplanetoken", "fixtures/list-organizations-200-00000-1.json"),
	)

	db, err := sql.Open("deltastream", "https://_:sometoken@api.deltastream.io/v2?")
	g.Expect(err).To(BeNil())

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT * FROM mview_table;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.Columns()).To(Equal([]string{"id", "name", "description", "profileImageURI", "createdAt"}))

	var (
		id      string
		discard any
	)
	for rows.Next() {
		err = rows.Scan(&id, &discard, &discard, &discard, &discard)
		g.Expect(err).To(BeNil())
	}
	g.Expect(rows.Err()).To(BeNil())
	g.Expect(id).To(Equal("0e0e3617-3cd6-4407-a189-97daf226c4d4"))
}
