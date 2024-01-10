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

	_ "embed"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
)

//go:embed fixtures/testattachment.blob
var attachmentData []byte

func TestAttachments(t *testing.T) {
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
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "LIST ORGANIZATIONS;", map[string][]byte{"test.blob": attachmentData}, "fixtures/list-organizations-200-00000-0.json"),
	)

	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
	g.Expect(err).To(BeNil())

	ctx := context.Background()
	ctx = WithAttachment(ctx, "test.blob", io.NopCloser(bytes.NewBuffer(attachmentData)))
	_, err = db.QueryContext(ctx, "LIST ORGANIZATIONS;")
	g.Expect(err).To(BeNil())
}
