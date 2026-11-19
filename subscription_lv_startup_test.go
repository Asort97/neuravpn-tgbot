package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestLVStartupPilot(t *testing.T) {
	const tag = "auto-bypass-2-ip-213.21.201.167"
	for _, tc := range []struct{ name, user, agent, outbound, proto, fallback, want string }{
		{"Обход 2 🇱🇻 Латвия", autoSelectPilotUser, "Happ/4", tag, "vless", "unavailable", tag},
		{"Обход 2 🇱🇻 Латвия", "other", "Happ/4", tag, "vless", "unavailable", "unavailable"},
		{"Обход 2 🇱🇻 Латвия", autoSelectPilotUser, "Incy", tag, "vless", "unavailable", "unavailable"},
		{"Обход 2 🇱🇻 Латвия", autoSelectPilotUser, "Happ", "missing", "vless", "unavailable", "unavailable"},
		{"Обход 2 🇱🇻 Латвия", autoSelectPilotUser, "Happ", tag, "freedom", "unavailable", "unavailable"},
		{"Обход 2 🇱🇻 Латвия", autoSelectPilotUser, "Happ", tag, "vless", "custom", "custom"},
		{"Германия ТЕСТ", autoSelectPilotUser, "Happ", tag, "vless", "unavailable", "unavailable"},
	} {
		route := map[string]any{"fallbackTag": tc.fallback}
		b, _ := json.Marshal([]any{map[string]any{"remarks": tc.name, "outbounds": []any{map[string]string{"tag": tc.outbound, "protocol": tc.proto}}, "routing": map[string]any{"balancers": []any{route}}}})
		out := applyLVStartupPilot(b, tc.user, tc.agent)
		var v []map[string]any
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		obj(obj(v[0]["routing"])["balancers"].([]any)[0])["fallbackTag"] = tc.want
		expected, _ := json.Marshal(v)
		if !bytes.Equal(out, expected) {
			t.Fatalf("%s: unexpected generated profile", tc.name)
		}
	}
}
