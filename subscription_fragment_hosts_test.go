package main

import (
	"encoding/json"
	"testing"
)

func TestHappFragmentSlowHosts(t *testing.T) {
	t.Setenv("SUBSCRIPTION_FRAGMENT_SLOW_HOSTS", "10.0.0.1, 10.0.0.2")
	t.Setenv("SUBSCRIPTION_FRAGMENT_LENGTH", "")
	t.Setenv("SUBSCRIPTION_FRAGMENT_INTERVAL", "")
	body := []byte(`[{"outbounds":[
	 {"protocol":"vless","settings":{"vnext":[{"address":"10.0.0.2","port":8443}]},"streamSettings":{"network":"xhttp","security":"reality"}},
	 {"protocol":"vless","settings":{"vnext":[{"address":"9.9.9.9","port":443}]},"streamSettings":{"network":"tcp","security":"reality"}}]}]`)
	var ps []map[string]any
	if err := json.Unmarshal(applyHappHelloProfile(body, "1", "Happ/4"), &ps); err != nil {
		t.Fatal(err)
	}
	get := func(i int) map[string]any {
		o := obj(ps[0]["outbounds"].([]any)[i])
		fm := obj(obj(o["streamSettings"])["finalmask"])
		return obj(fm["tcp"].([]any)[0])["settings"].(map[string]any)
	}
	if s := get(0); s["length"] != "10-20" || s["delay"] != "10-20" {
		t.Fatal("slow host must get small spread pieces", s)
	}
	if s := get(1); s["length"] != "100-200" || s["interval"] != "1-3" {
		t.Fatal("other hosts must get fast pieces", s)
	}
}
