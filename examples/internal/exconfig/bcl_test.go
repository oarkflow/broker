package exconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func parseString(s string) (*Node, error) {
	p := &parser{lx: newLexer(s)}
	root := &Node{Kind: "root", Fields: map[string][]Value{}}
	return root, p.parseInto(root)
}

func TestRawBacktickPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.bcl")
	want := `{
  "hello": "world",
  "nested": {"ok": true}
}`
	cfg := "codec_example \"raw\" {\n  payload `" + want + "`\n  payloads [`{\"a\":1}`, `{\"b\":2}`]\n}\n"
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	blk := FirstBlock(root, "codec_example")
	if blk == nil {
		t.Fatal("missing block")
	}
	got := blk.String("payload", "")
	if got != want {
		t.Fatalf("raw payload changed\nwant=%q\ngot =%q", want, got)
	}
	payloads := blk.Strings("payloads")
	if len(payloads) != 2 || !strings.Contains(payloads[0], `"a"`) || !strings.Contains(payloads[1], `"b"`) {
		t.Fatalf("raw payload list not parsed correctly: %#v", payloads)
	}
}

func TestPayloadBlockToJSON(t *testing.T) {
	root, err := parseString(`operation "x" {
  payload {
    to "user@example.com"
    amount 1499
    force_dlq true
    nested { ok true }
  }
  payloads {
    item { to "a@example.com" }
    item { to "b@example.com" }
  }
}`)
	if err != nil {
		t.Fatal(err)
	}
	op := FirstBlock(root, "operation")
	got := PayloadJSON(op, `{}`)
	for _, want := range []string{`"to":"user@example.com"`, `"amount":1499`, `"force_dlq":true`, `"nested":{"ok":true}`} {
		if !strings.Contains(got, want) {
			t.Fatalf("payload json %s missing %s", got, want)
		}
	}
	ps := PayloadsJSON(op)
	if len(ps) != 2 || !strings.Contains(ps[0], `a@example.com`) || !strings.Contains(ps[1], `b@example.com`) {
		t.Fatalf("payloads=%v", ps)
	}
}
