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
	"io"
)

type ctxkey string

var sqlRequestAttachmentsKey ctxkey = "sqlRequestAttachmentsKey"

type sqlRequestAttachments struct {
	attachments map[string]io.ReadCloser
}

func WithAttachment(ctx context.Context, paramName string, r io.ReadCloser) context.Context {
	if v := ctx.Value(sqlRequestAttachmentsKey); v != nil {
		if v, ok := v.(*sqlRequestAttachments); ok {
			v.attachments[paramName] = r
			return ctx
		}
	}
	return context.WithValue(ctx, sqlRequestAttachmentsKey, &sqlRequestAttachments{attachments: map[string]io.ReadCloser{paramName: r}})
}
