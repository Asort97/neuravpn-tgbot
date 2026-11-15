package main

import (
	"bytes"
	"net/http"
	"testing"
)

func TestINCYAutoSelectUserAgents(t *testing.T) {
	for _, tc := range []struct {
		ua   string
		want bool
	}{
		{"INCY/2.7.2/ios CFNetwork/3860.700.1 Darwin/25.6.0", true},
		{"INCY/3.9.9/android Dalvik/2.1.0", true},
		{"INCY/1.0/Windows", true},
		{" incy/2.7.2/iOS ", true},
		{"INCY", true},
		{"NotINCY/1.0", false},
		{"INCY-unrelated/1.0", false},
		{"Mozilla/5.0", false},
		{"v2rayNG/1.10", false},
	} {
		t.Run(tc.ua, func(t *testing.T) {
			if got := supportsAutoSelect(tc.ua, "*/*"); got != tc.want {
				t.Fatalf("supportsAutoSelect = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestINCYReceivesSameAutoSelectProfilesAsHapp(t *testing.T) {
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_PER_BYPASS_IP_ENABLED", "true")
	t.Setenv("SUBSCRIPTION_HYSTERIA_VARIANTS_ENABLED", "true")
	input := pilotURIs()
	want, _, err := autoSelectSubscription("5550418384", "Happ/5.9", "*/*", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, headers, err := autoSelectSubscription("5550418384", "INCY/2.7.2/ios", "*/*", input, nil)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("INCY and Happ profiles differ: %v", err)
	}
	if headers.Get("X-Neura-Autoselect") != "enabled" || headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatal("INCY did not receive the JSON subscription")
	}
}

func TestINCYRespectsAutoSelectSwitchAndHWIDDenials(t *testing.T) {
	input := pilotURIs()
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "true")
	for _, key := range []string{"X-Hwid-Max-Devices-Reached", "X-Hwid-Not-Supported"} {
		headers := make(http.Header)
		headers.Set(key, "true")
		got, h, err := autoSelectSubscription("5550418384", "INCY/2.7.2/ios", "*/*", input, headers)
		if err != nil || !bytes.Equal(got, input) || h.Get(key) != "true" || h.Get("X-Neura-Autoselect") != "" {
			t.Fatalf("HWID denial changed: %s, %v", key, err)
		}
	}
	t.Setenv("SUBSCRIPTION_AUTOSELECT_ENABLED", "false")
	got, headers, err := autoSelectSubscription("5550418384", "INCY/2.7.2/ios", "*/*", input, nil)
	if err != nil || !bytes.Equal(got, input) || headers.Get("X-Neura-Autoselect") != "" {
		t.Fatal("INCY bypassed the auto-select kill switch")
	}
}
