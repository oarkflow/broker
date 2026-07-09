package broker

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"
)

const (
	PermissionAdminAll     = "admin:*"
	PermissionBrokerRead   = "broker:read"
	PermissionPublish      = "message:publish"
	PermissionMessageRead  = "message:read"
	PermissionMessageAck   = "message:ack"
	PermissionMessageNack  = "message:nack"
	PermissionMessageMove  = "message:move"
	PermissionQueueRead    = "queue:read"
	PermissionQueueWrite   = "queue:write"
	PermissionQueueControl = "queue:control"
	PermissionQueuePurge   = "queue:purge"
	PermissionTopicRead    = "topic:read"
	PermissionTopicWrite   = "topic:write"
	PermissionStreamRead   = "stream:read"
	PermissionStreamWrite  = "stream:write"
	PermissionStreamCommit = "stream:commit"
	PermissionDLQRead      = "dlq:read"
	PermissionDLQReplay    = "dlq:replay"
	PermissionRouteRead    = "route:read"
	PermissionRouteWrite   = "route:write"
	PermissionNotification = "notification:read"
)

// AdminToken describes one API credential. Token values are intentionally not
// emitted through JSON. Use Permissions with exact strings or the admin:*
// wildcard for full access.
type AdminToken struct {
	Name        string   `json:"name,omitempty"`
	Token       string   `json:"-"`
	Permissions []string `json:"permissions,omitempty"`
	Tenants     []string `json:"tenants,omitempty"`
}

type AdminPrincipal struct {
	Name        string
	Token       string
	Permissions []string
	Tenants     []string
}

type adminPrincipalKey struct{}

func principalFromContext(ctx context.Context) AdminPrincipal {
	if p, ok := ctx.Value(adminPrincipalKey{}).(AdminPrincipal); ok {
		return p
	}
	return AdminPrincipal{Name: "anonymous"}
}

type ACL struct {
	Tokens []AdminToken
}

func NewACL(tokens ...AdminToken) *ACL { return &ACL{Tokens: tokens} }

func (a *ACL) Authenticate(raw string) (AdminPrincipal, bool) {
	if a == nil || len(a.Tokens) == 0 || raw == "" {
		return AdminPrincipal{}, false
	}
	for _, tok := range a.Tokens {
		if constantTimeEqual(raw, tok.Token) {
			name := tok.Name
			if name == "" {
				name = "token"
			}
			return AdminPrincipal{Name: name, Token: raw, Permissions: append([]string(nil), tok.Permissions...), Tenants: append([]string(nil), tok.Tenants...)}, true
		}
	}
	return AdminPrincipal{}, false
}

func (a *ACL) Authorize(p AdminPrincipal, permission string) bool {
	if permission == "" {
		return true
	}
	for _, perm := range p.Permissions {
		if perm == PermissionAdminAll || perm == permission {
			return true
		}
		if strings.HasSuffix(perm, ":*") {
			prefix := strings.TrimSuffix(perm, "*")
			if strings.HasPrefix(permission, prefix) {
				return true
			}
		}
	}
	return false
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		// Still compare a same-sized dummy buffer so mismatched lengths do not
		// become a cheap oracle for prefix guessing.
		if len(a) > len(b) {
			b = strings.Repeat("\x00", len(a))
		} else {
			a = strings.Repeat("\x00", len(b))
		}
		return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 && false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func bearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if len(auth) < len("Bearer ") || !strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(auth[len("Bearer "):])
}

type AdminAuditEvent struct {
	Time       time.Time `json:"time"`
	Principal  string    `json:"principal"`
	RemoteAddr string    `json:"remote_addr,omitempty"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Action     string    `json:"action"`
	Allowed    bool      `json:"allowed"`
	Status     int       `json:"status"`
	Reason     string    `json:"reason,omitempty"`
}

type AdminAuditLogger interface {
	LogAdmin(context.Context, AdminAuditEvent)
}

type NotificationAuditLogger struct{ Bus *NotificationBus }

func (l NotificationAuditLogger) LogAdmin(ctx context.Context, e AdminAuditEvent) {
	if l.Bus == nil {
		return
	}
	sev := SeverityInfo
	if !e.Allowed || e.Status >= 400 {
		sev = SeverityWarn
	}
	l.Bus.Publish(Notification{Type: "admin.audit", Source: "admin", Severity: sev, Timestamp: e.Time, Data: map[string]any{"principal": e.Principal, "remote_addr": e.RemoteAddr, "method": e.Method, "path": e.Path, "action": e.Action, "allowed": e.Allowed, "status": e.Status, "reason": e.Reason}})
}
