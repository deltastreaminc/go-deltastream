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
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Stmt             = &statement{}
	_ driver.StmtExecContext  = &statement{}
	_ driver.StmtQueryContext = &statement{}
)

type statement struct {
	c      *Conn
	query  string
	isOpen bool
}

// Close implements driver.Stmt.
func (s *statement) Close() error {
	s.isOpen = false
	return nil
}

// Exec implements driver.Stmt.
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	if !s.isOpen {
		return nil, &ErrStatementClosed{}
	}
	return s.c.Exec(s.query, args)
}

type result struct {
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *result) LastInsertId() (int64, error) {
	return -1, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *result) RowsAffected() (int64, error) {
	return -1, nil
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if !s.isOpen {
		return nil, &ErrStatementClosed{}
	}

	return s.c.ExecContext(ctx, s.query, args)
}

// NumInput implements driver.Stmt.
func (s *statement) NumInput() int {
	if !s.isOpen {
		return 0
	}
	return 0
}

// Query implements driver.Stmt.
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	if !s.isOpen {
		return nil, &ErrStatementClosed{}
	}
	return s.c.Query(s.query, args)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if !s.isOpen {
		return nil, &ErrStatementClosed{}
	}

	return s.c.QueryContext(ctx, s.query, args)
}
