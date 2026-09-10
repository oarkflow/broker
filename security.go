package broker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	PermissionAdminAll      = "admin:*"
	PermissionBrokerRead    = "broker:read"
	PermissionPublish       = "message:publish"
	PermissionMessageRead   = "message:read"
	PermissionMessageAck    = "message:ack"
	PermissionMessageNack   = "message:nack"
	PermissionMessageMove   = "message:move"
	PermissionQueueRead     = "queue:read"
	PermissionQueueWrite    = "queue:write"
	PermissionQueueControl  = "queue:control"
	PermissionQueuePurge    = "queue:purge"
	PermissionTopicRead     = "topic:read"
	PermissionTopicWrite    = "topic:write"
	PermissionStreamRead    = "stream:read"
	PermissionStreamWrite   = "stream:write"
	PermissionStreamCommit  = "stream:commit"
	PermissionDLQRead       = "dlq:read"
	PermissionDLQReplay     = "dlq:replay"
	PermissionRouteRead     = "route:read"
	PermissionRouteWrite    = "route:write"
	PermissionNotification  = "notification:read"
	PermissionConsumerBan   = "consumer:ban"
	PermissionConsumerUnban = "consumer:unban"
)

// AdminToken describes one API credential. Token values are intentionally not
// emitted through JSON. Use Permissions with exact strings or the admin:*
// wildcard for full access.
type AdminToken struct {
	Name        string    `json:"name,omitempty"`
	Token       string    `json:"-"`
	Permissions []string  `json:"permissions,omitempty"`
	Tenants     []string  `json:"tenants,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	Revoked     bool      `json:"revoked,omitempty"`
	tokenHash   [sha256.Size]byte
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

func scopedPrincipalFromContext(ctx context.Context) (AdminPrincipal, bool) {
	p, ok := ctx.Value(adminPrincipalKey{}).(AdminPrincipal)
	return p, ok
}

// resolveTenant applies the authenticated principal's tenant scope. Embedded
// calls without an HTTP principal remain unrestricted. A credential scoped to
// one tenant may omit tenant_id; it is filled in safely. Multi-tenant scoped
// credentials must select one of their allowed tenants explicitly.
func resolveTenant(ctx context.Context, requested string) (string, error) {
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) == 0 {
		return requested, nil
	}
	for _, tenant := range p.Tenants {
		if tenant == "*" {
			return requested, nil
		}
	}
	if requested == "" {
		if len(p.Tenants) == 1 {
			return p.Tenants[0], nil
		}
		return "", errf(ErrUnauthorized, "tenant_id is required for this credential")
	}
	for _, tenant := range p.Tenants {
		if tenant == requested {
			return requested, nil
		}
	}
	return "", errf(ErrUnauthorized, "credential is not authorized for tenant %q", requested)
}

func tenantAllowed(ctx context.Context, tenant string) bool {
	_, err := resolveTenant(ctx, tenant)
	return err == nil
}

func restrictedTenant(ctx context.Context) (string, bool) {
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) != 1 || p.Tenants[0] == "*" {
		return "", false
	}
	return p.Tenants[0], true
}

type ACL struct {
	mu     sync.RWMutex
	Tokens []AdminToken
}

func NewACL(tokens ...AdminToken) *ACL {
	a := &ACL{}
	for _, token := range tokens {
		a.addTokenLocked(token)
	}
	return a
}

func (a *ACL) AddToken(token AdminToken) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.addTokenLocked(token)
	a.mu.Unlock()
}

func (a *ACL) addTokenLocked(token AdminToken) {
	token.tokenHash = sha256.Sum256([]byte(token.Token))
	token.Token = ""
	token.Permissions = append([]string(nil), token.Permissions...)
	token.Tenants = append([]string(nil), token.Tenants...)
	a.Tokens = append(a.Tokens, token)
}

// Revoke invalidates every credential with the supplied logical name.
func (a *ACL) Revoke(name string) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	found := false
	for i := range a.Tokens {
		if a.Tokens[i].Name == name {
			a.Tokens[i].Revoked = true
			found = true
		}
	}
	return found
}

func (a *ACL) Authenticate(raw string) (AdminPrincipal, bool) {
	if a == nil || raw == "" {
		return AdminPrincipal{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	candidate := sha256.Sum256([]byte(raw))
	now := time.Now()
	for _, tok := range a.Tokens {
		stored := tok.tokenHash
		if stored == ([sha256.Size]byte{}) && tok.Token != "" {
			stored = sha256.Sum256([]byte(tok.Token))
		}
		if subtle.ConstantTimeCompare(candidate[:], stored[:]) == 1 && !tok.Revoked && (tok.ExpiresAt.IsZero() || now.Before(tok.ExpiresAt)) {
			name := tok.Name
			if name == "" {
				name = "token"
			}
			return AdminPrincipal{Name: name, Permissions: append([]string(nil), tok.Permissions...), Tenants: append([]string(nil), tok.Tenants...)}, true
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
		return false
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
