package instruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestGuidesAreComplete(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, p := range guidePlatforms {
		if len(p.apps) < 2 {
			t.Fatalf("%s offers %d apps, want a choice", p.id, len(p.apps))
		}
		for _, a := range p.apps {
			if data := "ig:s:" + p.id + ":" + a.id + ":9"; len(data) > 64 {
				t.Fatalf("callback %q exceeds 64 bytes", data)
			}
			hasKey := false
			for i, s := range a.steps {
				if s.media != "" {
					if _, err := os.Stat(filepath.Join(root, s.media)); err != nil {
						t.Fatalf("%s/%s step %d: missing media %s", p.id, a.id, i, s.media)
					}
				}
				// Telegram caps captions at 1024 characters; leave room for the title and key.
				if n := utf8.RuneCountInString(s.caption); n > 700 {
					t.Fatalf("%s/%s step %d: caption is %d characters", p.id, a.id, i, n)
				}
				if strings.Contains(strings.ToLower(s.caption), "v2raytun") {
					t.Fatalf("%s/%s step %d still mentions v2RayTun", p.id, a.id, i)
				}
				hasKey = hasKey || s.key
			}
			if !hasKey {
				t.Fatalf("%s/%s never shows the key", p.id, a.id)
			}
		}
	}
}

func TestGuideCallbacksParse(t *testing.T) {
	if !IsGuidePlatform("linux") || IsGuidePlatform("nav_menu") {
		t.Fatal("platform detection")
	}
	if !IsGuideStepCallback("ig:s:ios:happ:2") || IsGuideStepCallback("ig:p:ios") {
		t.Fatal("step detection")
	}
}
