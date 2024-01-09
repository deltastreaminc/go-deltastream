package godeltastream

import (
	"bytes"
	"database/sql"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
)

func TestDatatypes(t *testing.T) {
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
		mockSubmitStatementsResponser(g, http.StatusOK, "sometoken", "TEST DATATYPES;", "fixtures/test-datatypes-200-00000-4.json"),
	)

	db, err := sql.Open("deltastream", "https://api.deltastream.io/v2?token=sometoken")
	g.Expect(err).To(BeNil())

	rows, err := db.Query("TEST DATATYPES;")
	g.Expect(err).To(BeNil())

	g.Expect(rows.Columns()).To(Equal([]string{
		"VARCHAR",
		"TINYINT",
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"FLOAT",
		"DOUBLE",
		"DECIMAL",
		"TIMESTAMP",
		"TIMESTAMP_TZ",
		"DATE",
		"TIME",
		"TIMESTAMP_LTZ",
		"VARBINARY",
		"BYTES",
		"ARRAY",
		"MAP",
		"STRUCT",
		"BOOLEAN",
		"VARCHAR_NULLABLE",
		"TINYINT_NULLABLE",
		"SMALLINT_NULLABLE",
		"INTEGER_NULLABLE",
		"BIGINT_NULLABLE",
		"FLOAT_NULLABLE",
		"DOUBLE_NULLABLE",
		"DECIMAL_NULLABLE",
		"TIMESTAMP_NULLABLE",
		"TIMESTAMP_TZ_NULLABLE",
		"DATE_NULLABLE",
		"TIME_NULLABLE",
		"TIMESTAMP_LTZ_NULLABLE",
		"VARBINARY_NULLABLE",
		"BYTES_NULLABLE",
		"ARRAY_NULLABLE",
		"MAP_NULLABLE",
		"STRUCT_NULLABLE",
		"BOOLEAN_NULLABLE",
	}))
	ctypes, err := rows.ColumnTypes()
	g.Expect(err).To(BeNil())
	g.Expect(ctypes[0].Name()).To(Equal("VARCHAR"))

	var (
		varchar                string
		tinyint                int64
		smallint               int64
		integer                int64
		bigint                 int64
		floatv                 float64
		doublev                float64
		decimal                float64
		timestamp              time.Time
		timestamp_tz           time.Time
		date                   time.Time
		timev                  time.Time
		timestamp_ltz          time.Time
		varbinary              []byte
		bytes                  []byte
		array                  string
		mapv                   string
		structv                string
		boolean                bool
		varchar_nullable       *string
		tinyint_nullable       *int64
		smallint_nullable      *int64
		integer_nullable       *int64
		bigint_nullable        *int64
		float_nullable         *float64
		double_nullable        *float64
		decimal_nullable       *float64
		timestamp_nullable     *time.Time
		timestamp_tz_nullable  *time.Time
		date_nullable          *time.Time
		time_nullable          *time.Time
		timestamp_ltz_nullable *time.Time
		varbinary_nullable     *[]byte
		bytes_nullable         *[]byte
		array_nullable         *string
		map_nullable           *string
		struct_nullable        *string
		boolean_nullable       *bool
	)

	for rows.Next() {
		err = rows.Scan(&varchar,
			&tinyint,
			&smallint,
			&integer,
			&bigint,
			&floatv,
			&doublev,
			&decimal,
			&timestamp,
			&timestamp_tz,
			&date,
			&timev,
			&timestamp_ltz,
			&varbinary,
			&bytes,
			&array,
			&mapv,
			&structv,
			&boolean,
			&varchar_nullable,
			&tinyint_nullable,
			&smallint_nullable,
			&integer_nullable,
			&bigint_nullable,
			&float_nullable,
			&double_nullable,
			&decimal_nullable,
			&timestamp_nullable,
			&timestamp_tz_nullable,
			&date_nullable,
			&time_nullable,
			&timestamp_ltz_nullable,
			&varbinary_nullable,
			&bytes_nullable,
			&array_nullable,
			&map_nullable,
			&struct_nullable,
			&boolean_nullable)
		g.Expect(err).To(BeNil())
	}
	g.Expect(rows.Err()).To(BeNil())
}
