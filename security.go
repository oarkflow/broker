package broker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker/cluster"
)

func validateListenerSecurity(protocol string, allowInsecure, hasTLS, hasAuthentication bool, authenticationKind string) error {
	if allowInsecure {
		return nil
	}
	if !hasTLS {
		return fmt.Errorf("%s: TLS is required outside insecure development mode", protocol)
	}
	if !hasAuthentication {
		if authenticationKind == "" {
			authenticationKind = "authentication"
		}
		return fmt.Errorf("%s: %s is required outside insecure development mode", protocol, authenticationKind)
	}
	return nil
}

func tlsConfigVerifiesClientCertificates(cfg *tls.Config) bool {
	if cfg == nil || cfg.ClientCAs == nil {
		return false
	}
	return cfg.ClientAuth == tls.VerifyClientCertIfGiven || cfg.ClientAuth == tls.RequireAndVerifyClientCert
}

const (
	PermissionAdminAll      = "admin:*"
	PermissionBrokerRead    = "broker:read"
	PermissionStatsRead     = "stats:read"
	PermissionMetricsRead   = "metrics:read"
	PermissionClusterRead   = "cluster:read"
	PermissionClusterWrite  = "cluster:write"
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

// HasPermission reports whether the principal has the specified permission.
func (p AdminPrincipal) HasPermission(perm string) bool {
	for _, cp := range p.Permissions {
		if cp == perm || cp == PermissionAdminAll || cp == "*" {
			return true
		}
	}
	return false
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
// calls without an HTTP principal remain unrestricted, because in-process code has
// no credential to present; see Config.ACL for where the trust boundary sits. A
// credential scoped to one tenant may omit tenant_id; it is filled in safely.
// Multi-tenant scoped credentials must select one of their allowed tenants
// explicitly.
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
		if tenantWithinScope(tenant, allowed) {
			return true
		}
	}
	return false
}

func tenantWithinScope(tenant, scope string) bool {
	if scope == "*" {
		return true
	}
	return tenant != "" && (scope == tenant || strings.HasPrefix(tenant, scope+".") || strings.HasPrefix(tenant, scope+":") || strings.HasPrefix(tenant, scope+"/"))
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

// ResolveTenant applies the authenticated principal's tenant scope. Storage
// adapters outside this module can use it to enforce the same tenant default
// and authorization rules as the built-in stores.
func ResolveTenant(ctx context.Context, requested string) (string, error) {
	return resolveTenant(ctx, requested)
}

// TenantAllowed reports whether a tenant identifier is visible to the principal
// attached to ctx. It is intended for storage and protocol adapters.
func TenantAllowed(ctx context.Context, tenant string) bool { return tenantAllowed(ctx, tenant) }

// HasRestrictedTenantScope reports whether ctx is limited to one or more named
// tenants rather than being unscoped or explicitly global.
func HasRestrictedTenantScope(ctx context.Context) bool { return hasRestrictedTenantScope(ctx) }

func visibleOffsetGroup(ctx context.Context, stored string) (string, bool) {
	tenant, group, encoded := parseTenantScopedKey(stored)
	p, scoped := scopedPrincipalFromContext(ctx)
	if !scoped || len(p.Tenants) == 0 {
		if encoded {
			return displayOffsetGroup(tenant, group, stored), true
		}
		return stored, true
	}
	for _, allowed := range p.Tenants {
		if allowed == "*" {
			if encoded {
				return displayOffsetGroup(tenant, group, stored), true
			}
			return stored, true
		}
	}
	if !encoded {
		// Keep visibility compatible with un-migrated snapshots and callers
		// holding an older tenant+NUL+group key.
		tenant, group, encoded = strings.Cut(stored, "\x00")
	}
	if !encoded || tenant == "" || group == "" {
		return "", false
	}
	for _, allowed := range p.Tenants {
		if tenant == allowed {
			if len(p.Tenants) == 1 {
				return group, true
			}
			return displayOffsetGroup(tenant, group, stored), true
		}
	}
	return "", false
}

func displayOffsetGroup(tenant, group, encoded string) string {
	if strings.ContainsRune(tenant, '\x00') || strings.ContainsRune(group, '\x00') {
		return encoded
	}
	if tenant == "" {
		return group
	}
	return tenant + "\x00" + group
}

func parseTenantScopedKey(value string) (tenant, key string, ok bool) {
	if len(value) < 3 || value[0] != 't' {
		return "", "", false
	}
	length, rest, found := strings.Cut(value[1:], ":")
	if !found {
		return "", "", false
	}
	n, err := strconv.Atoi(length)
	if err != nil || n < 0 || n > len(rest) {
		return "", "", false
	}
	return rest[:n], rest[n:], true
}

type ACL struct {
	mu                 sync.RWMutex
	Tokens             []AdminToken
	controllerQuorum   *cluster.ControllerQuorum
	syncMu             sync.Mutex
	refreshMu          sync.Mutex
	controllerRevision uint64
	controllerSynced   bool
	quotaMu            sync.Mutex
	quotas             map[string]*principalQuota
}

func (a *ACL) hasUsableTokenCredential() bool {
	if a == nil {
		return false
	}
	a.refreshControllerPrincipals()
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now()
	for _, token := range a.Tokens {
		if !token.Revoked && (token.tokenSet || token.Token != "") &&
			(token.ExpiresAt.IsZero() || now.Before(token.ExpiresAt)) {
			return true
		}
	}
	return false
}

func (a *ACL) hasUsableCertificateCredential() bool {
	if a == nil {
		return false
	}
	a.refreshControllerPrincipals()
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now()
	for _, token := range a.Tokens {
		if token.Revoked || (!token.ExpiresAt.IsZero() && !now.Before(token.ExpiresAt)) {
			continue
		}
		for _, identity := range token.CertificateIdentities {
			if validACLIdentity(identity) {
				return true
			}
		}
	}
	return false
}

func validACLIdentity(identity string) bool {
	prefix, value, ok := strings.Cut(strings.TrimSpace(identity), ":")
	if !ok || value == "" || value != strings.TrimSpace(value) {
		return false
	}
	switch prefix {
	case "cn", "dns", "uri", "email":
		return true
	case "ip":
		return net.ParseIP(value) != nil
	default:
		return false
	}
}

func (a *ACL) hasUsableCredential(certificateAuth bool) bool {
	return a.hasUsableTokenCredential() || (certificateAuth && a.hasUsableCertificateCredential())
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

// SetControllerQuorum makes locally applied controller ACL metadata the
// authoritative credential set once the quorum has at least one principal.
// Empty metadata preserves bootstrap credentials until they are replicated.
func (a *ACL) SetControllerQuorum(controller *cluster.ControllerQuorum) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.controllerQuorum = controller
	a.controllerRevision = 0
	a.controllerSynced = false
	a.mu.Unlock()
	a.refreshControllerPrincipals()
}

// SyncControllerQuorum seeds an empty controller ACL table from the configured
// bootstrap ACL, or applies the existing replicated ACL table locally.
func (a *ACL) SyncControllerQuorum(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	a.mu.RLock()
	controller := a.controllerQuorum
	bootstrap := a.clusterPrincipalsLocked()
	a.mu.RUnlock()
	if controller == nil {
		return nil
	}
	principals, revision, err := controller.ListACLPrincipalsLinearizableWithRevision(ctx)
	if err != nil {
		return err
	}
	known := make(map[string]struct{}, len(principals))
	for _, principal := range principals {
		known[principal.Name] = struct{}{}
	}
	for _, principal := range bootstrap {
		if _, exists := known[principal.Name]; exists {
			continue
		}
		if err := controller.PutACLPrincipal(ctx, principal, ""); err != nil {
			return err
		}
		known[principal.Name] = struct{}{}
	}
	principals, revision, err = controller.ListACLPrincipalsLinearizableWithRevision(ctx)
	if err != nil {
		return err
	}
	a.refreshMu.Lock()
	principals, revision = controller.ListACLPrincipalsWithRevision()
	a.replaceControllerPrincipals(principals)
	a.mu.Lock()
	a.controllerRevision = revision
	a.controllerSynced = true
	a.mu.Unlock()
	a.refreshMu.Unlock()
	return nil
}

func (a *ACL) refreshControllerPrincipals() {
	a.mu.RLock()
	controller := a.controllerQuorum
	lastRevision := a.controllerRevision
	synced := a.controllerSynced
	a.mu.RUnlock()
	if controller == nil {
		return
	}
	if synced && controller.ACLRevision() == lastRevision {
		return
	}
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()
	a.mu.RLock()
	controller = a.controllerQuorum
	lastRevision = a.controllerRevision
	synced = a.controllerSynced
	a.mu.RUnlock()
	if controller == nil {
		return
	}
	if synced && controller.ACLRevision() == lastRevision {
		return
	}
	principals, revision := controller.ListACLPrincipalsWithRevision()
	if len(principals) > 0 {
		a.replaceControllerPrincipals(principals)
	}
	a.mu.Lock()
	a.controllerRevision = revision
	a.controllerSynced = true
	a.mu.Unlock()
}

func (a *ACL) replaceControllerPrincipals(principals []cluster.ACLPrincipal) {
	tokens := make([]AdminToken, 0, len(principals))
	now := time.Now()
	for _, principal := range principals {
		digest, err := hex.DecodeString(principal.TokenHash)
		if principal.TokenHash != "" && (err != nil || len(digest) != sha256.Size) {
			continue
		}
		token := AdminToken{
			Name: principal.Name, Permissions: append([]string(nil), principal.Permissions...),
			Tenants:               append([]string(nil), principal.Tenants...),
			CertificateIdentities: append([]string(nil), principal.CertificateIdentities...),
			RequestsPerSecond:     principal.RequestsPerSecond, Burst: principal.Burst,
			MaxConcurrent: principal.MaxConcurrent, ExpiresAt: principal.ExpiresAt,
			Revoked: principal.Revoked, tokenSet: principal.TokenHash != "",
		}
		copy(token.tokenHash[:], digest)
		if !cluster.ACLPrincipalActive(principal, now) {
			token.Revoked = true
		}
		tokens = append(tokens, token)
	}
	a.mu.Lock()
	a.Tokens = tokens
	a.mu.Unlock()
}

func (a *ACL) clusterPrincipalsLocked() []cluster.ACLPrincipal {
	out := make([]cluster.ACLPrincipal, 0, len(a.Tokens))
	for i, token := range a.Tokens {
		name := token.Name
		if name == "" {
			name = fmt.Sprintf("credential-%d", i+1)
		}
		principal := cluster.ACLPrincipal{
			Name: name, Permissions: append([]string(nil), token.Permissions...),
			Tenants:               append([]string(nil), token.Tenants...),
			CertificateIdentities: append([]string(nil), token.CertificateIdentities...),
			RequestsPerSecond:     token.RequestsPerSecond, Burst: token.Burst,
			MaxConcurrent: token.MaxConcurrent, ExpiresAt: token.ExpiresAt,
			Revoked: token.Revoked,
		}
		if token.tokenSet {
			principal.TokenHash = hex.EncodeToString(token.tokenHash[:])
		}
		out = append(out, principal)
	}
	return out
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
	a.refreshControllerPrincipals()
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

func authenticateAdminCredential(acl *ACL, raw string, cert *x509.Certificate) (AdminPrincipal, bool) {
	if principal, ok := acl.AuthenticateCertificate(cert); ok {
		return principal, true
	}
	return acl.Authenticate(raw)
}

func sameAdminPrincipal(a, b AdminPrincipal) bool {
	return a.Name == b.Name && slices.Equal(a.Permissions, b.Permissions) &&
		slices.Equal(a.Tenants, b.Tenants) && a.RequestsPerSecond == b.RequestsPerSecond &&
		a.Burst == b.Burst && a.MaxConcurrent == b.MaxConcurrent
}

// AuthenticateCertificate maps a verified TLS client certificate to a named
// ACL principal. Identity entries use cn:, dns:, uri:, email:, or ip: prefixes.
func (a *ACL) AuthenticateCertificate(cert *x509.Certificate) (AdminPrincipal, bool) {
	if a == nil || cert == nil {
		return AdminPrincipal{}, false
	}
	a.refreshControllerPrincipals()
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

// AdmitRate charges one application operation against the principal's shared
// request token bucket without consuming a connection-concurrency slot. Use it
// for messages carried over already-admitted long-lived protocol connections.
func (a *ACL) AdmitRate(principal AdminPrincipal) error {
	if a == nil || principal.RequestsPerSecond <= 0 {
		return nil
	}
	a.quotaMu.Lock()
	defer a.quotaMu.Unlock()
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
		return &RetryError{Class: RetryThrottled, Message: "principal request rate exceeded", RetryAfter: retry, Cause: ErrThrottled}
	}
	quota.tokens--
	return nil
}

// AdmitConnection charges the principal's request bucket for opening the
// connection and enforces MaxConcurrent across protocols. The returned release
// function must be called once the connection terminates.
func (a *ACL) AdmitConnection(principal AdminPrincipal) (func(), error) {
	if a == nil {
		return func() {}, nil
	}
	if err := a.AdmitRate(principal); err != nil {
		return nil, err
	}
	if principal.MaxConcurrent <= 0 {
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
	if quota.concurrent >= principal.MaxConcurrent {
		a.quotaMu.Unlock()
		return nil, &RetryError{Class: RetryThrottled, Message: "principal connection concurrency limit reached", RetryAfter: time.Second, Cause: ErrThrottled}
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

// WithAdminPrincipal attaches an authenticated principal to a context so that
// in-process code runs under the same tenant scoping a listener would apply.
//
// This is the supported way for an embedder to apply per-tenant rules to its own
// calls. Without it, in-process calls are unscoped, which is the documented trust
// boundary rather than an oversight: see Config.ACL.
//
// A listener that authenticates a request must use this (or an equivalent
// internal helper) rather than trusting callers to set the value themselves.
func WithAdminPrincipal(ctx context.Context, p AdminPrincipal) context.Context {
	return context.WithValue(ctx, adminPrincipalKey{}, p)
}

// AdminPrincipalFromContext returns the principal attached to ctx, if any.
func AdminPrincipalFromContext(ctx context.Context) (AdminPrincipal, bool) {
	return scopedPrincipalFromContext(ctx)
}
