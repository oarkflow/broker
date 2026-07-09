package exconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Value struct {
	Kind string
	Text string
	List []Value
}

type Node struct {
	Kind     string
	Name     string
	Fields   map[string][]Value
	Children []*Node
}

func Load(path string) (*Node, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := &parser{lx: newLexer(string(b))}
	root := &Node{Kind: "root", Fields: map[string][]Value{}}
	if err := p.parseInto(root); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return root, nil
}

func FirstBlock(root *Node, kind string) *Node {
	if root == nil {
		return nil
	}
	for _, c := range root.Children {
		if c.Kind == kind {
			return c
		}
	}
	return nil
}

func (n *Node) Blocks(kind string) []*Node {
	if n == nil {
		return nil
	}
	var out []*Node
	for _, c := range n.Children {
		if c.Kind == kind {
			out = append(out, c)
		}
	}
	return out
}

func (n *Node) Block(kind, name string) *Node {
	if n == nil {
		return nil
	}
	for _, c := range n.Children {
		if c.Kind == kind && c.Name == name {
			return c
		}
	}
	return nil
}

func (n *Node) Values(key string) []Value {
	if n == nil || n.Fields == nil {
		return nil
	}
	return n.Fields[key]
}

func (n *Node) Strings(key string) []string {
	vals := n.Values(key)
	if len(vals) == 0 {
		return nil
	}
	if len(vals) == 1 && vals[0].Kind == "list" {
		out := make([]string, 0, len(vals[0].List))
		for _, v := range vals[0].List {
			out = append(out, v.Text)
		}
		return out
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if v.Kind == "list" {
			for _, item := range v.List {
				out = append(out, item.Text)
			}
			continue
		}
		out = append(out, v.Text)
	}
	return out
}

func (n *Node) String(key, def string) string {
	ss := n.Strings(key)
	if len(ss) == 0 || ss[0] == "" {
		return def
	}
	return ss[0]
}

func (n *Node) Bool(key string, def bool) bool {
	s := strings.ToLower(strings.TrimSpace(n.String(key, "")))
	if s == "" {
		return def
	}
	return s == "true" || s == "1" || s == "yes" || s == "on"
}

func (n *Node) Int(key string, def int) int {
	s := strings.TrimSpace(n.String(key, ""))
	if s == "" {
		return def
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
}

func (n *Node) Duration(key string, def time.Duration) time.Duration {
	s := strings.TrimSpace(n.String(key, ""))
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func ParseBytes(s string, def int64) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return def
	}
	mul := int64(1)
	for _, suffix := range []struct {
		S string
		M int64
	}{
		{"KB", 1 << 10}, {"K", 1 << 10},
		{"MB", 1 << 20}, {"M", 1 << 20},
		{"GB", 1 << 30}, {"G", 1 << 30},
	} {
		if strings.HasSuffix(s, suffix.S) {
			mul = suffix.M
			s = strings.TrimSpace(strings.TrimSuffix(s, suffix.S))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n * mul
}

// PayloadJSON returns a JSON payload from a native BCL payload block first,
// falling back to a payload field for backwards compatibility. Prefer:
//
//	payload {
//	  to "user@example.com"
//	  amount 1499
//	  nested { enabled true }
//	}
//
// This keeps payloads represented as BCL objects instead of escaped strings.
func PayloadJSON(n *Node, def string) string {
	if n == nil {
		return MustJSONPayload(def)
	}
	if blocks := n.Blocks("payload"); len(blocks) > 0 {
		b, err := json.Marshal(NodeObject(blocks[0]))
		if err == nil {
			return string(b)
		}
	}
	return MustJSONPayload(n.String("payload", def))
}

// PayloadsJSON returns a JSON payload list from a native BCL payloads block:
//
//	payloads {
//	  item { to "a@example.com" }
//	  item { to "b@example.com" }
//	}
func PayloadsJSON(n *Node) []string {
	if n == nil {
		return nil
	}
	if blocks := n.Blocks("payloads"); len(blocks) > 0 {
		var out []string
		for _, item := range blocks[0].Blocks("item") {
			b, err := json.Marshal(NodeObject(item))
			if err == nil {
				out = append(out, string(b))
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return n.Strings("payloads")
}

// NodeObject converts a BCL block into a JSON-compatible object. Repeated
// fields/children become arrays. Scalar words are parsed as bool/int/float
// where appropriate, otherwise they remain strings.
func NodeObject(n *Node) map[string]any {
	out := map[string]any{}
	if n == nil {
		return out
	}
	for k, vals := range n.Fields {
		if k == "args" {
			continue
		}
		if len(vals) == 1 {
			out[k] = valueAny(vals[0])
			continue
		}
		arr := make([]any, 0, len(vals))
		for _, v := range vals {
			arr = append(arr, valueAny(v))
		}
		out[k] = arr
	}
	children := map[string][]*Node{}
	for _, c := range n.Children {
		children[c.Kind] = append(children[c.Kind], c)
	}
	for k, list := range children {
		if len(list) == 1 {
			out[k] = NodeObject(list[0])
			continue
		}
		arr := make([]any, 0, len(list))
		for _, c := range list {
			arr = append(arr, NodeObject(c))
		}
		out[k] = arr
	}
	return out
}

func valueAny(v Value) any {
	if v.Kind == "list" {
		arr := make([]any, 0, len(v.List))
		for _, item := range v.List {
			arr = append(arr, valueAny(item))
		}
		return arr
	}
	s := strings.TrimSpace(v.Text)
	if s == "" {
		return ""
	}
	ls := strings.ToLower(s)
	if ls == "true" || ls == "false" {
		return ls == "true"
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && strings.ContainsAny(s, ".eE") {
		return f
	}
	return v.Text
}

func MustJSONPayload(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return `{}`
	}
	return s
}

type tokenType int

const (
	tEOF tokenType = iota
	tWord
	tString
	tLBrace
	tRBrace
	tLBracket
	tRBracket
	tLParen
	tRParen
	tComma
	tNewline
)

type token struct {
	typ tokenType
	val string
	pos int
}

type lexer struct {
	s string
	i int
}

func newLexer(s string) *lexer { return &lexer{s: s} }

func (l *lexer) next() token {
	for l.i < len(l.s) {
		ch := l.s[l.i]
		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.i++
			continue
		}
		if ch == '#' {
			for l.i < len(l.s) && l.s[l.i] != '\n' {
				l.i++
			}
			continue
		}
		if ch == '/' && l.i+1 < len(l.s) && l.s[l.i+1] == '/' {
			for l.i < len(l.s) && l.s[l.i] != '\n' {
				l.i++
			}
			continue
		}
		break
	}
	if l.i >= len(l.s) {
		return token{typ: tEOF, pos: l.i}
	}
	pos := l.i
	ch := l.s[l.i]
	switch ch {
	case '\n', ';':
		l.i++
		return token{typ: tNewline, val: "\n", pos: pos}
	case '{':
		l.i++
		return token{typ: tLBrace, val: "{", pos: pos}
	case '}':
		l.i++
		return token{typ: tRBrace, val: "}", pos: pos}
	case '[':
		l.i++
		return token{typ: tLBracket, val: "[", pos: pos}
	case ']':
		l.i++
		return token{typ: tRBracket, val: "]", pos: pos}
	case '(':
		l.i++
		return token{typ: tLParen, val: "(", pos: pos}
	case ')':
		l.i++
		return token{typ: tRParen, val: ")", pos: pos}
	case ',':
		l.i++
		return token{typ: tComma, val: ",", pos: pos}
	case '"':
		l.i++
		var b strings.Builder
		for l.i < len(l.s) {
			c := l.s[l.i]
			l.i++
			if c == '"' {
				break
			}
			if c == '\\' && l.i < len(l.s) {
				n := l.s[l.i]
				l.i++
				switch n {
				case 'n':
					b.WriteByte('\n')
				case 't':
					b.WriteByte('\t')
				case 'r':
					b.WriteByte('\r')
				case '"', '\\':
					b.WriteByte(n)
				default:
					b.WriteByte(n)
				}
				continue
			}
			b.WriteByte(c)
		}
		return token{typ: tString, val: b.String(), pos: pos}
	case '`':
		// Raw BCL strings are intentionally not escaped. They are useful for
		// JSON payloads, SQL, scripts, and any example/config value that should
		// be written exactly as it is sent to the broker. A raw string may span
		// multiple lines and is terminated by the next backtick.
		l.i++
		start := l.i
		for l.i < len(l.s) && l.s[l.i] != '`' {
			l.i++
		}
		val := l.s[start:l.i]
		if l.i < len(l.s) && l.s[l.i] == '`' {
			l.i++
		}
		return token{typ: tString, val: val, pos: pos}
	default:
		start := l.i
		for l.i < len(l.s) {
			r := rune(l.s[l.i])
			if unicode.IsSpace(r) || strings.ContainsRune("{}[](),;", r) {
				break
			}
			l.i++
		}
		return token{typ: tWord, val: l.s[start:l.i], pos: pos}
	}
}

type parser struct {
	lx  *lexer
	buf *token
}

func (p *parser) next() token {
	if p.buf != nil {
		t := *p.buf
		p.buf = nil
		return t
	}
	return p.lx.next()
}
func (p *parser) peek() token    { t := p.next(); p.buf = &t; return t }
func (p *parser) unread(t token) { p.buf = &t }

func (p *parser) parseInto(n *Node) error {
	for {
		t := p.next()
		switch t.typ {
		case tEOF:
			return nil
		case tRBrace:
			return nil
		case tNewline:
			continue
		case tWord:
			if err := p.parseStmt(n, t.val); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected token %q at %d", t.val, t.pos)
		}
	}
}

func (p *parser) parseStmt(parent *Node, key string) error {
	vals := []Value{}
	for {
		t := p.next()
		switch t.typ {
		case tEOF, tNewline:
			parent.addField(key, vals)
			return nil
		case tRBrace:
			parent.addField(key, vals)
			p.unread(t)
			return nil
		case tLBrace:
			child := &Node{Kind: key, Fields: map[string][]Value{}}
			if len(vals) > 0 {
				child.Name = vals[0].Text
				child.Fields["args"] = vals
			}
			if err := p.parseInto(child); err != nil {
				return err
			}
			parent.Children = append(parent.Children, child)
			return nil
		default:
			p.unread(t)
			v, err := p.parseValue()
			if err != nil {
				return err
			}
			vals = append(vals, v)
		}
	}
}

func (n *Node) addField(key string, vals []Value) {
	if n.Fields == nil {
		n.Fields = map[string][]Value{}
	}
	if len(vals) == 0 {
		n.Fields[key] = append(n.Fields[key], Value{Kind: "bool", Text: "true"})
		return
	}
	n.Fields[key] = append(n.Fields[key], vals...)
}

func (p *parser) parseValue() (Value, error) {
	t := p.next()
	switch t.typ {
	case tString:
		return Value{Kind: "string", Text: t.val}, nil
	case tWord:
		if t.val == "env" && p.peek().typ == tLParen {
			p.next()
			name, err := p.parseValue()
			if err != nil {
				return Value{}, err
			}
			def := Value{Kind: "string", Text: ""}
			if p.peek().typ == tComma {
				p.next()
				def, err = p.parseValue()
				if err != nil {
					return Value{}, err
				}
			}
			if p.peek().typ == tRParen {
				p.next()
			}
			if v := os.Getenv(name.Text); v != "" {
				return Value{Kind: "string", Text: v}, nil
			}
			return Value{Kind: "string", Text: def.Text}, nil
		}
		return Value{Kind: "word", Text: t.val}, nil
	case tLBracket:
		var list []Value
		for {
			if p.peek().typ == tRBracket {
				p.next()
				break
			}
			if p.peek().typ == tComma || p.peek().typ == tNewline {
				p.next()
				continue
			}
			v, err := p.parseValue()
			if err != nil {
				return Value{}, err
			}
			list = append(list, v)
			if p.peek().typ == tComma {
				p.next()
			}
		}
		return Value{Kind: "list", List: list}, nil
	default:
		return Value{}, fmt.Errorf("expected value, got %q at %d", t.val, t.pos)
	}
}
