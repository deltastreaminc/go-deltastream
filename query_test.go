package godeltastream

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
			case "attachment":
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
		if h, ok := r.Header["Authorization"]; !ok || h[0] != "Bearer sometoken" {
			return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(`{ "message": "no token" }`))}, nil
		}

		f, err := os.OpenFile(fixture, os.O_RDONLY, 0600)
		g.Expect(err).To(BeNil())
		h := http.Header{}
		h.Add("Content-Type", "application/json")
		return &http.Response{StatusCode: statusCode, Body: f, Header: h}, nil
	}
}

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

	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
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
	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
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

	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
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
