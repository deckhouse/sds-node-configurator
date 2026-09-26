/*
Copyright 2026 Flant JSC

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

package leader

import (
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/sds-common-lib/logger"
)

type stubState struct {
	leader bool
	addr   string
}

func (s stubState) IsLeader() bool        { return s.leader }
func (s stubState) LeaderAddress() string { return s.addr }
func (s stubState) Identity() string      { return "standby_10.0.0.2" }

func reservationPath(path string) bool { return path == "/scheduler/filter" }

type recorder struct {
	calls   int
	path    string
	body    string
	fwdFrom string
}

func (rec *recorder) handler(reply string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.calls++
		rec.path = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		rec.body = string(b)
		rec.fwdFrom = r.Header.Get(ForwardedHeader)
		_, _ = io.WriteString(w, reply)
	})
}

func do(t *testing.T, h http.Handler, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"pod":"p"}`))
	for k, v := range header {
		req.Header[k] = v
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestRouter(t *testing.T) {
	leaderRec := &recorder{}
	leaderSrv := httptest.NewTLSServer(leaderRec.handler("from-leader"))
	defer leaderSrv.Close()
	leaderAddr := leaderSrv.Listener.Addr().String()

	t.Run("the leader serves reservation routes itself", func(t *testing.T) {
		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{leader: true}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		w := do(t, rt, "/scheduler/filter", nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "local", w.Body.String())
		assert.Equal(t, 1, local.calls)
	})

	t.Run("a standby forwards reservation routes to the leader", func(t *testing.T) {
		*leaderRec = recorder{}
		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{addr: leaderAddr}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		w := do(t, rt, "/scheduler/filter", nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "from-leader", w.Body.String())
		assert.Zero(t, local.calls, "a standby must not touch its own reservations")
		assert.Equal(t, "/scheduler/filter", leaderRec.path)
		assert.Equal(t, `{"pod":"p"}`, leaderRec.body)
		assert.Equal(t, "standby_10.0.0.2", leaderRec.fwdFrom)
	})

	t.Run("a standby serves the other routes itself", func(t *testing.T) {
		*leaderRec = recorder{}
		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{addr: leaderAddr}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		for _, path := range []string{"/cache", "/stat", "/status"} {
			w := do(t, rt, path, nil)
			assert.Equal(t, "local", w.Body.String(), path)
		}
		assert.Equal(t, 3, local.calls)
		assert.Zero(t, leaderRec.calls)
	})

	t.Run("a standby refuses a forwarded request instead of forwarding it again", func(t *testing.T) {
		*leaderRec = recorder{}
		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{addr: leaderAddr}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		w := do(t, rt, "/scheduler/filter", http.Header{ForwardedHeader: {"other_10.0.0.3"}})
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Zero(t, local.calls)
		assert.Zero(t, leaderRec.calls)
	})

	t.Run("no leader known means 503, not a local answer", func(t *testing.T) {
		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		w := do(t, rt, "/scheduler/filter", nil)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Zero(t, local.calls)
	})

	t.Run("an unreachable leader means 503, not a local answer", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		deadAddr := l.Addr().String()
		require.NoError(t, l.Close())

		local := &recorder{}
		rt := NewRouter(logger.NewNop(), stubState{addr: deadAddr}, local.handler("local"), reservationPath, leaderSrv.Client().Transport)

		w := do(t, rt, "/scheduler/filter", nil)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Zero(t, local.calls)
	})
}

// TestNewLeaderTransport dials the leader by IP and checks its certificate
// against a name the certificate carries, as the extender does with the pod IP
// and the Service name.
func TestNewLeaderTransport(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	// The httptest certificate names example.com.
	tr, err := NewLeaderTransport(caPEM, "example.com")
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	tr, err = NewLeaderTransport(caPEM, "sds-common-scheduler-extender")
	require.NoError(t, err)
	_, err = (&http.Client{Transport: tr}).Get(srv.URL)
	require.Error(t, err, "a certificate that does not carry the expected name must be rejected")

	_, err = NewLeaderTransport([]byte("not a pem"), "example.com")
	require.Error(t, err)
}
