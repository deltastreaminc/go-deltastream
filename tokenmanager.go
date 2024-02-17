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
	"fmt"
	"time"

	"golang.org/x/oauth2"
)

type TokenManager interface {
	// If access token is empty will call through login flow. If access token
	// has expired it will request a new one using refresh token
	GetToken(context.Context) (string, error)
	// From oauth2.TokenSource
	Token() (*oauth2.Token, error)
}

type AuthClient interface {
	Login(context.Context) (*TokenInfo, error)
	RefreshToken(ctx context.Context, refreshToken string) (*TokenInfo, error)
}

type TokenInfo struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	// Note that auth0 returns expires_in (seconds). We translate to a time when getting
	// token back from server
	ExpiresAt uint64 `json:"expires_at"`
}

var _ oauth2.TokenSource = &tokenManager{}

type tokenManager struct {
	authClient AuthClient
	tokenInfo  *TokenInfo
	ctx        context.Context
}

func NewStaticTokenManager(ctx context.Context, token string) TokenManager {
	ti := &TokenInfo{
		AccessToken: token,
	}
	return &tokenManager{
		tokenInfo: ti,
		ctx:       ctx,
	}
}
func NewTokenManager(ctx context.Context, authClient AuthClient) TokenManager {
	return &tokenManager{
		authClient: authClient,
		tokenInfo:  &TokenInfo{},
		ctx:        ctx,
	}
}

func (t *tokenManager) Token() (*oauth2.Token, error) {
	_, err := t.GetToken(t.ctx)
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{
		AccessToken:  t.tokenInfo.AccessToken,
		RefreshToken: t.tokenInfo.RefreshToken,
		Expiry:       time.Unix(int64(t.tokenInfo.ExpiresAt), 0),
	}, nil
}

func (t *tokenManager) GetToken(ctx context.Context) (string, error) {
	if t.tokenInfo.AccessToken == "" {
		ti, err := t.authClient.Login(ctx)
		if err != nil {
			return "", err
		}
		t.tokenInfo = ti
		return t.tokenInfo.AccessToken, nil
	}
	if t.tokenInfo.RefreshToken != "" {
		exp := time.Unix(int64(t.tokenInfo.ExpiresAt), 0)
		if !exp.IsZero() && exp.Before(time.Now()) {
			if t.tokenInfo.RefreshToken == "" {
				return "", fmt.Errorf("missing refresh_token")
			}
			refreshed, err := t.authClient.RefreshToken(ctx, t.tokenInfo.RefreshToken)
			if err != nil {
				return "", err
			}
			t.tokenInfo = refreshed
		}
	}

	return t.tokenInfo.AccessToken, nil
}
