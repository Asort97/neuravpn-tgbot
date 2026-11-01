package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func deviceWebRequest(body, nonce string) *http.Request {
	r := httptest.NewRequest("POST", webDevicesPath, strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:1234"
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	r.Header.Set("X-Device-Time", stamp)
	r.Header.Set("X-Device-Nonce", nonce)
	r.Header.Set("X-Device-Signature", webDevicesMAC("test-secret", "POST\n"+webDevicesPath+"\n"+stamp+"\n"+nonce+"\n"+body))
	return r
}

func TestWebDevicesAuthenticationAndReplay(t *testing.T) {
	setDeviceTestProviders(t)
	h := newWebDevicesHandler(nil, "test-secret")
	body := `{"user_id":"42","action":"list"}`
	nonce := strings.Repeat("ab", 16)
	for _, kind := range []string{"remote", "signature", "expired", "body"} {
		r := deviceWebRequest(body, nonce)
		switch kind {
		case "remote":
			r.RemoteAddr = "203.0.113.1:1234"
		case "signature":
			r.Header.Set("X-Device-Signature", "forged")
		case "expired":
			r.Header.Set("X-Device-Time", "1")
		case "body":
			r.Body = http.NoBody
		}
		w := httptest.NewRecorder()
		h(w, r)
		if w.Code != 403 {
			t.Fatalf("%s accepted: %d", kind, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h(w, deviceWebRequest(body, nonce))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	h(w, deviceWebRequest(body, nonce))
	if w.Code != 409 {
		t.Fatal("replay accepted")
	}
}

func TestWebDevicesDeletesMergedOwnedRecordsOnly(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	b, fb := devicePanel(t, "b", 2, "0123456789ab")
	setDeviceTestProviders(t, b)
	h := newWebDevicesHandler(a, "test-secret")
	call := func(body, nonce string) map[string]any {
		w := httptest.NewRecorder()
		h(w, deviceWebRequest(body, strings.Repeat(nonce, 16)))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var out map[string]any
		if json.Unmarshal(w.Body.Bytes(), &out) != nil {
			t.Fatal("bad response")
		}
		return out
	}
	out := call(`{"user_id":"42","action":"list"}`, "ab")
	devices := out["devices"].([]any)
	if len(devices) != 1 {
		t.Fatal("merged device not grouped")
	}
	id := devices[0].(map[string]any)["id"].(string)
	foreign := webDevicesMAC("test-secret", "device\n999\nhwid:0123456789ab")
	call(`{"user_id":"42","action":"delete","id":"`+foreign+`"}`, "bc")
	if fa.deletes+fb.deletes != 0 {
		t.Fatal("foreign device deleted")
	}
	fb.failDelete = true
	out = call(`{"user_id":"42","action":"delete","id":"`+id+`"}`, "cd")
	if out["result"] != "queued" || len(out["devices"].([]any)) != 0 || fa.deletes != 0 || fb.deletes != 0 {
		t.Fatal("deletion request waited for panels")
	}
	drainDeviceDeletions(t, a)
	if fa.deletes != 1 || fb.deletes != 0 || len(userStore.(*deletionTestStore).jobs) != 1 {
		t.Fatal("native deletion retry missing")
	}
	fb.failDelete = false
	drainDeviceDeletions(t, a)
	out = call(`{"user_id":"42","action":"list"}`, "de")
	if len(out["devices"].([]any)) != 0 || fb.deletes != 1 {
		t.Fatal("merged delete failed")
	}
}

func TestWebDevicesRechecksOwnership(t *testing.T) {
	a, fa := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	fa.foreign = true
	h := newWebDevicesHandler(a, "test-secret")
	id := webDevicesMAC("test-secret", "device\n42\nhwid:0123456789ab")
	w := httptest.NewRecorder()
	h(w, deviceWebRequest(`{"user_id":"42","action":"delete","id":"`+id+`"}`, strings.Repeat("ab", 16)))
	if w.Code != 200 || fa.deletes != 0 || !strings.Contains(w.Body.String(), `"result":"absent"`) {
		t.Fatal("ownership not rechecked")
	}
}

func TestWebDeviceDeleteDoesNotClaimAbsentAfterLostSnapshotAndPanelFailure(t *testing.T) {
	a, f := devicePanel(t, "a", 1, "0123456789ab")
	setDeviceTestProviders(t)
	h := newWebDevicesHandler(a, "test-secret")
	w := httptest.NewRecorder()
	h(w, deviceWebRequest(`{"user_id":"42","action":"list"}`, strings.Repeat("aa", 16)))
	deviceSnapshots.Lock()
	deviceSnapshots.values = make(map[string]deviceSnapshot)
	deviceSnapshots.Unlock()
	f.unsupported = true
	id := webDevicesMAC("test-secret", "device\n42\nhwid:0123456789ab")
	w = httptest.NewRecorder()
	h(w, deviceWebRequest(`{"user_id":"42","action":"delete","id":"`+id+`"}`, strings.Repeat("bb", 16)))
	if w.Code != 503 || strings.Contains(w.Body.String(), `"absent"`) || f.deletes != 0 {
		t.Fatal("failed lookup reported absence", w.Code, w.Body.String())
	}
}
