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
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/deltastreaminc/go-deltastream/apiv2"
	"github.com/google/uuid"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Rows                           = &resultSetRows{}
	_ driver.RowsColumnTypeScanType         = &resultSetRows{}
	_ driver.RowsColumnTypeDatabaseTypeName = &resultSetRows{}
	_ driver.RowsColumnTypeNullable         = &resultSetRows{}
	// _ driver.RowsColumnTypeLength           = &rows{}
	// _ driver.RowsColumnTypePrecisionScale   = &rows{}
)

var typeMap map[string]reflect.Type

func init() {
	typeMap = map[string]reflect.Type{
		"VARCHAR":       reflect.TypeOf(""),
		"TINYINT":       reflect.TypeOf(int64(0)),
		"SMALLINT":      reflect.TypeOf(int64(0)),
		"INTEGER":       reflect.TypeOf(int64(0)),
		"BIGINT":        reflect.TypeOf(int64(0)),
		"FLOAT":         reflect.TypeOf(float64(0)),
		"DOUBLE":        reflect.TypeOf(float64(0)),
		"DECIMAL":       reflect.TypeOf(float64(0)),
		"TIMESTAMP":     reflect.TypeOf(time.Now()),
		"TIMESTAMP_TZ":  reflect.TypeOf(time.Now()),
		"DATE":          reflect.TypeOf(time.Now()),
		"TIME":          reflect.TypeOf(time.Now()),
		"TIMESTAMP_LTZ": reflect.TypeOf(time.Now()),
		"VARBINARY":     reflect.TypeOf([]byte{}),
		"BYTES":         reflect.TypeOf([]byte{}),
		"ARRAY":         reflect.TypeOf(""),
		"MAP":           reflect.TypeOf(""),
		"STRUCT":        reflect.TypeOf(""),
		"BOOLEAN":       reflect.TypeOf(true),
	}
}

type ResultSetConn interface {
	getStatement(ctx context.Context, statementID uuid.UUID, partitionID int32) (rs *apiv2.ResultSet, err error)
}

type resultSetRows struct {
	conn ResultSetConn
	ctx  context.Context

	currentRowIdx       int32
	currentPartitionIdx int32

	currentResultSet *apiv2.ResultSet
}

func (r *resultSetRows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	if index < 0 || index >= len(r.currentResultSet.Metadata.Columns) {
		return false, false
	}
	md := r.currentResultSet.Metadata.Columns[index]
	return md.Nullable, true
}

func (r *resultSetRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.currentResultSet.Metadata.Columns) {
		return ""
	}
	md := r.currentResultSet.Metadata.Columns[index]
	return md.Type
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *resultSetRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.currentResultSet.Metadata.Columns) {
		return nil
	}
	md := r.currentResultSet.Metadata.Columns[index]
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

// Close implements driver.Rows.
func (r *resultSetRows) Close() error {
	r.conn = nil
	return nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *resultSetRows) Columns() []string {
	cols := []string{}
	for _, c := range r.currentResultSet.Metadata.Columns {
		cols = append(cols, c.Name)
	}

	return cols
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *resultSetRows) Next(dest []driver.Value) error {
	rowIdx, partIdx := r.calcPartitionIdx(r.currentRowIdx + 1)
	if partIdx == -1 {
		return io.EOF
	}
	if partIdx != r.currentPartitionIdx {
		resp, err := r.conn.getStatement(r.ctx, r.currentResultSet.StatementID, int32(partIdx))
		if err != nil {
			return err
		}
		r.currentPartitionIdx = partIdx
		r.currentResultSet = resp
	}
	r.currentRowIdx += 1
	rowData := (*r.currentResultSet.Data)[rowIdx]
	if len(rowData) != len(dest) {
		return &ErrClientError{message: fmt.Sprintf("number of columns does not match size of result slice. expected %d, got %d", len(rowData), len(dest))}
	}

	var err error
	for idx, col := range r.currentResultSet.Metadata.Columns {
		switch {
		case rowData[idx] == nil:
			dest[idx] = nil
		case strings.HasPrefix(col.Type, "VARCHAR") || strings.HasPrefix(col.Type, "ARRAY") || strings.HasPrefix(col.Type, "MAP") || strings.HasPrefix(col.Type, "STRUCT"):
			dest[idx] = *rowData[idx]
		case col.Type == "TINYINT" || col.Type == "SMALLINT" || col.Type == "INTEGER" || col.Type == "BIGINT":
			dest[idx], err = strconv.ParseInt(*rowData[idx], 10, 64)
			if err != nil {
				return err
			}
		case col.Type == "FLOAT" || col.Type == "DOUBLE" || strings.HasPrefix(col.Type, "DECIMAL"):
			dest[idx], err = strconv.ParseFloat(*rowData[idx], 64)
			if err != nil {
				return err
			}
		case strings.HasPrefix(col.Type, "TIME") || col.Type == "DATE":
			dest[idx], err = parseTime(*rowData[idx], col.Type)
			if err != nil {
				return err
			}
		case col.Type == "VARBINARY" || col.Type == "BYTES":
			dest[idx], err = base64.StdEncoding.DecodeString(*rowData[idx])
			if err != nil {
				return err
			}
		case col.Type == "BOOLEAN":
			dest[idx] = strings.ToLower(*rowData[idx]) == "true"
		}
	}
	return nil
}

func (r *resultSetRows) calcPartitionIdx(rowIdx int32) (row, part int32) {
	for pIdx, p := range r.currentResultSet.Metadata.PartitionInfo {
		if rowIdx < p.RowCount {
			return rowIdx, int32(pIdx)
		}
		rowIdx = rowIdx - p.RowCount
	}
	return -1, -1
}

func parseTime(s, colType string) (time.Time, error) {
	if colType == `DATE` {
		return time.Parse(`2006-01-02`, s)
	}

	switch {
	case strings.HasPrefix(colType, `TIMESTAMP`):
		sspl := strings.Split(s, " ")
		if len(sspl) != 2 {
			return time.Now(), fmt.Errorf("invalid timestamp")
		}
		timePart := sspl[1]
		containsNano := strings.Contains(timePart, ".")
		containsTZ := strings.Contains(timePart, "Z") || strings.Contains(timePart, "+") || strings.Contains(timePart, "-")

		layout := "2006-01-02 15:04:05"
		if containsNano {
			layout += ".999999999"
		}
		if containsTZ {
			layout += "Z0700"
		}
		return time.Parse(layout, s)
	case colType == `TIME` || strings.HasPrefix(colType, "TIME("):
		containsNano := strings.Contains(s, ".")
		containsTZ := strings.Contains(s, "Z") || strings.Contains(s, "+") || strings.Contains(s, "-")
		layout := "15:04:05"
		if containsNano {
			layout += ".999999999"
		}
		if containsTZ {
			layout += "Z0700"
		}
		return time.Parse(layout, s)
	default:
		return time.Now(), fmt.Errorf("unsupported column type")
	}
}
