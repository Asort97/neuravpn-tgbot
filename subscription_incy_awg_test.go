package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIncyAWGPilot(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "pilot")
	cfg := base64.RawURLEncoding.EncodeToString([]byte("[Interface]\nPrivateKey = dummy\n[Peer]\nPublicKey = dummy"))
	raw, _ := json.Marshal(map[string]any{"expires": now.Add(time.Hour).Unix(), "config": cfg})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	body := []byte(`[{"remarks":"original"}]`)
	for _, tc := range []struct {
		user, ua string
		at       time.Time
		want     bool
	}{
		{autoSelectPilotUser, "Incy/3.9.9", now, true},
		{"other", "Incy/3.9.9", now, false}, {autoSelectPilotUser, "Happ", now, false},
		{autoSelectPilotUser, "Incy/3.9.9 Windows", now, false}, {autoSelectPilotUser, "Incy", now.Add(time.Hour), false},
	} {
		out := appendIncyAWGPilot(body, tc.user, tc.ua, path, tc.at)
		var items []json.RawMessage
		if json.Unmarshal(out, &items) != nil || (len(items) == 2) != tc.want || string(items[0]) != `{"remarks":"original"}` {
			t.Fatal(tc)
		}
		if tc.want {
			var container struct {
				Type    string
				Version int
				Servers []struct{ Name, Config string }
			}
			if json.Unmarshal(items[1], &container) != nil || container.Type != "amneziawg" || container.Version != 1 || len(container.Servers) != 1 || container.Servers[0].Config != cfg {
				t.Fatal("wrong AWG container")
			}
		} else if string(out) != string(body) {
			t.Fatal("changed nonpilot subscription")
		}
	}
}

func TestIncyLatviaPilotIndependentOfGermany(t *testing.T) {
	now := time.Now()
	de := filepath.Join(t.TempDir(), "incy-awg.json")
	lv := filepath.Join(filepath.Dir(de), "incy-awg-lv.json")
	body := []byte(`[{"remarks":"original"}]`)
	cfg := base64.RawURLEncoding.EncodeToString([]byte("[Interface]\nPrivateKey = local\n[Peer]\nPublicKey = local"))
	raw, _ := json.Marshal(map[string]any{"expires": now.Add(time.Hour).Unix(), "config": cfg})
	for i, path := range []string{lv, de} {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		out := appendIncyAWGPilot(body, autoSelectPilotUser, "Incy/4 Android", de, now)
		var items []struct {
			Servers []struct{ Name, Config string }
		}
		if json.Unmarshal(out, &items) != nil || len(items) != 2 || len(items[1].Servers) != i+1 ||
			items[1].Servers[i].Name != "Латвия · AmneziaWG (тест)" || items[1].Servers[i].Config != cfg {
			t.Fatal("missing or unexpected trial servers")
		}
		if string(appendIncyAWGPilot(body, "other", "Incy", de, now)) != string(body) ||
			string(appendIncyAWGPilot(body, autoSelectPilotUser, "Happ", de, now)) != string(body) {
			t.Fatal("pilot leaked outside the mobile Incy trial")
		}
	}
}
