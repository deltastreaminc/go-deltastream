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
	"database/sql"
	"io"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
)

func TestPing(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "https://api.deltastream.io/v2/version", func(r *http.Request) (*http.Response, error) {
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(`{ "major": 1, "minor": 0, "patch": 0 }`))}, nil
	})

	g := gomega.NewWithT(t)
	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2")
	g.Expect(err).To(BeNil())

	err = db.Ping()
	g.Expect(err).Should(MatchError(&ErrClientError{message: "no api token provided"}))

	db, err = sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
	g.Expect(err).To(BeNil())

	err = db.Ping()
	g.Expect(err).Should(BeNil())
}

func TestTransactionRetrunsError(t *testing.T) {
	g := gomega.NewWithT(t)
	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
	g.Expect(err).To(BeNil())

	_, err = db.Begin()
	g.Expect(err).Should(MatchError(&ErrClientError{message: "feature is not supported"}))
}
