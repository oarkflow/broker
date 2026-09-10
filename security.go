package broker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
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
	Name                  string    `json:"name,omitempty"`
	Token                 string    `json:"-"`
	Permissions           []string  `json:"permissions,omitempty"`
	Tenants               []string  `json:"tenants,omitempty"`
	CertificateIdentities []string  `json:"certificate_identities,omitempty"`
	RequestsPerSecond     int       `json:"requests_per_second,omitempty"`
	Burst                 int       `json:"burst,omitempty"`
	MaxConcurrent         int       `json:"max_concurrent,omitempty"`
	ExpiresAt             time.Time `json:"expires_at,omitempty"`
	Revoked               bool      `json:"revoked,omitempty"`
	tokenHash             [sha256.Size]byte
	tokenSet              bool
}

type AdminPrincipal struct {
	Name              string
	Token             string
	Permissions       []string
	Tenants           []string
	RequestsPerSecond int
	Burst             int
	MaxConcurrent     int
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
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) == 0 {
		return true
	}
	for _, allowed := range p.Tenants {
		if allowed == "*" {
			return true
		}
		if tenant != "" && allowed == tenant {
			return true
		}
	}
	return false
}

func restrictedTenant(ctx context.Context) (string, bool) {
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) != 1 || p.Tenants[0] == "*" {
		return "", false
	}
	return p.Tenants[0], true
}

// hasRestrictedTenantScope reports whether the authenticated credential is
// limited to any explicit set of tenants. Global control-plane resources must
// reject such credentials until those resources have native tenant ownership.
func hasRestrictedTenantScope(ctx context.Context) bool {
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) == 0 {
		return false
	}
	for _, tenant := range p.Tenants {
		if tenant == "*" {
			return false
		}
	}
	return true
}

func visibleOffsetGroup(ctx context.Context, stored string) (string, bool) {
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) == 0 {
		return stored, true
	}
	for _, tenant := range p.Tenants {
		if tenant == "*" {
			return stored, true
		}
	}
	tenant, group, ok := strings.Cut(stored, "\x00")
	if !ok || tenant == "" || group == "" {
		return "", false
	}
	for _, allowed := range p.Tenants {
		if tenant == allowed {
			if len(p.Tenants) == 1 {
				return group, true
			}
			return stored, true
		}
	}
	return "", false
}

type ACL struct {
	mu      sync.RWMutex
	Tokens  []AdminToken
	quotaMu sync.Mutex
	quotas  map[string]*principalQuota
}

type principalQuota struct {
	tokens     float64
	last       time.Time
	concurrent int
}

func NewACL(tokens ...AdminToken) *ACL {
	a := &ACL{quotas: make(map[string]*principalQuota)}
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
	token.tokenSet = token.Token != ""
	if token.tokenSet {
		token.tokenHash = sha256.Sum256([]byte(token.Token))
	}
	token.Token = ""
	token.Permissions = append([]string(nil), token.Permissions...)
	token.Tenants = append([]string(nil), token.Tenants...)
	token.CertificateIdentities = append([]string(nil), token.CertificateIdentities...)
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
		if !tok.tokenSet && tok.Token == "" {
			continue
		}
		stored := tok.tokenHash
		if stored == ([sha256.Size]byte{}) && tok.Token != "" {
			stored = sha256.Sum256([]byte(tok.Token))
		}
		if subtle.ConstantTimeCompare(candidate[:], stored[:]) == 1 && !tok.Revoked && (tok.ExpiresAt.IsZero() || now.Before(tok.ExpiresAt)) {
			name := tok.Name
			if name == "" {
				name = "token"
			}
			return principalForToken(name, tok), true
		}
	}
	return AdminPrincipal{}, false
}

// AuthenticateCertificate maps a verified TLS client certificate to a named
// ACL principal. Identity entries use cn:, dns:, uri:, email:, or ip: prefixes.
func (a *ACL) AuthenticateCertificate(cert *x509.Certificate) (AdminPrincipal, bool) {
	if a == nil || cert == nil {
		return AdminPrincipal{}, false
	}
	identities := certificateIdentities(cert)
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now()
	for _, token := range a.Tokens {
		if token.Revoked || (!token.ExpiresAt.IsZero() && !now.Before(token.ExpiresAt)) {
			continue
		}
		for _, allowed := range token.CertificateIdentities {
			for _, presented := range identities {
				if constantTimeEqual(allowed, presented) {
					name := token.Name
					if name == "" {
						name = presented
					}
					return principalForToken(name, token), true
				}
			}
		}
	}
	return AdminPrincipal{}, false
}

func principalForToken(name string, token AdminToken) AdminPrincipal {
	return AdminPrincipal{Name: name, Permissions: append([]string(nil), token.Permissions...), Tenants: append([]string(nil), token.Tenants...), RequestsPerSecond: token.RequestsPerSecond, Burst: token.Burst, MaxConcurrent: token.MaxConcurrent}
}

// Admit applies the authenticated principal's shared request-rate and
// concurrency limits. The returned release function must be called exactly
// once after the request or connection finishes.
func (a *ACL) Admit(principal AdminPrincipal) (func(), error) {
	if a == nil || (principal.RequestsPerSecond <= 0 && principal.MaxConcurrent <= 0) {
		return func() {}, nil
	}
	a.quotaMu.Lock()
	if a.quotas == nil {
		a.quotas = make(map[string]*principalQuota)
	}
	quota := a.quotas[principal.Name]
	if quota == nil {
		burst := principal.Burst
		if burst <= 0 {
			burst = principal.RequestsPerSecond
		}
		quota = &principalQuota{tokens: float64(burst), last: time.Now()}
		a.quotas[principal.Name] = quota
	}
	if principal.MaxConcurrent > 0 && quota.concurrent >= principal.MaxConcurrent {
		a.quotaMu.Unlock()
		return nil, &RetryError{Class: RetryThrottled, Message: "principal concurrency limit reached", RetryAfter: time.Second, Cause: ErrThrottled}
	}
	if principal.RequestsPerSecond > 0 {
		now := time.Now()
		burst := principal.Burst
		if burst <= 0 {
			burst = principal.RequestsPerSecond
		}
		quota.tokens += now.Sub(quota.last).Seconds() * float64(principal.RequestsPerSecond)
		if quota.tokens > float64(burst) {
			quota.tokens = float64(burst)
		}
		quota.last = now
		if quota.tokens < 1 {
			retry := time.Duration((1-quota.tokens)/float64(principal.RequestsPerSecond)*float64(time.Second)) + time.Millisecond
			a.quotaMu.Unlock()
			return nil, &RetryError{Class: RetryThrottled, Message: "principal request rate exceeded", RetryAfter: retry, Cause: ErrThrottled}
		}
		quota.tokens--
	}
	quota.concurrent++
	a.quotaMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			a.quotaMu.Lock()
			if quota.concurrent > 0 {
				quota.concurrent--
			}
			a.quotaMu.Unlock()
		})
	}, nil
}

func certificateIdentities(cert *x509.Certificate) []string {
	identities := make([]string, 0, 1+len(cert.DNSNames)+len(cert.EmailAddresses)+len(cert.IPAddresses)+len(cert.URIs))
	if cert.Subject.CommonName != "" {
		identities = append(identities, "cn:"+cert.Subject.CommonName)
	}
	for _, value := range cert.DNSNames {
		identities = append(identities, "dns:"+value)
	}
	for _, value := range cert.EmailAddresses {
		identities = append(identities, "email:"+value)
	}
	for _, value := range cert.IPAddresses {
		identities = append(identities, "ip:"+value.String())
	}
	for _, value := range cert.URIs {
		identities = append(identities, "uri:"+value.String())
	}
	return identities
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
