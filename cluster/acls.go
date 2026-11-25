package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxACLPrincipalNameBytes = 255
	maxACLTokenBytes         = 4096
	maxACLPolicyValues       = 4096
	maxACLPolicyValueBytes   = 512
)

// PutACLPrincipal replicates a principal policy. Supply token only when
// configuring a bearer credential; only its SHA-256 digest is stored.
func (cq *ControllerQuorum) PutACLPrincipal(ctx context.Context, principal ACLPrincipal, token string) error {
	if token != "" {
		if len(token) > maxACLTokenBytes {
			return ErrInvalidACLPrincipal
		}
		digest := sha256.Sum256([]byte(token))
		principal.TokenHash = hex.EncodeToString(digest[:])
	}
	normalized, err := normalizeACLPrincipal(principal)
	if err != nil || normalized.Revoked {
		return ErrInvalidACLPrincipal
	}
	return cq.propose(ctx, MetadataCommand{Op: OpPutACLPrincipal, ACLPrincipal: &normalized})
}

// RevokeACLPrincipal keeps the replicated record for auditability and makes
// the revocation visible after a quorum commit.
func (cq *ControllerQuorum) RevokeACLPrincipal(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxACLPrincipalNameBytes || !utf8.ValidString(name) {
		return ErrInvalidACLPrincipal
	}
	return cq.propose(ctx, MetadataCommand{Op: OpRevokeACLPrincipal, ACLPrincipalName: name})
}

func (cq *ControllerQuorum) GetACLPrincipal(name string) (ACLPrincipal, error) {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	principal := cq.meta.ACLPrincipals[name]
	if principal == nil {
		return ACLPrincipal{}, ErrACLPrincipalNotFound
	}
	return cloneACLPrincipal(principal), nil
}

func (cq *ControllerQuorum) GetACLPrincipalLinearizable(ctx context.Context, name string) (ACLPrincipal, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return ACLPrincipal{}, err
	}
	return cq.GetACLPrincipal(name)
}

func (cq *ControllerQuorum) ListACLPrincipals() []ACLPrincipal {
	principals, _ := cq.ListACLPrincipalsWithRevision()
	return principals
}

// ListACLPrincipalsWithRevision returns one consistent local metadata view.
func (cq *ControllerQuorum) ListACLPrincipalsWithRevision() ([]ACLPrincipal, uint64) {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	out := make([]ACLPrincipal, 0, len(cq.meta.ACLPrincipals))
	for _, principal := range cq.meta.ACLPrincipals {
		out = append(out, cloneACLPrincipal(principal))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, cq.meta.ACLRevision
}

// ACLRevision changes whenever a principal policy is applied and is included
// in controller snapshots so authentication caches can cheaply detect updates.
func (cq *ControllerQuorum) ACLRevision() uint64 {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	return cq.meta.ACLRevision
}

func (cq *ControllerQuorum) ListACLPrincipalsLinearizable(ctx context.Context) ([]ACLPrincipal, error) {
	principals, _, err := cq.ListACLPrincipalsLinearizableWithRevision(ctx)
	return principals, err
}

// ListACLPrincipalsLinearizableWithRevision returns a quorum-confirmed view
// and the revision that identifies that exact local snapshot.
func (cq *ControllerQuorum) ListACLPrincipalsLinearizableWithRevision(ctx context.Context) ([]ACLPrincipal, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, 0, err
	}
	principals, revision := cq.ListACLPrincipalsWithRevision()
	return principals, revision, nil
}

func normalizeACLPrincipal(principal ACLPrincipal) (ACLPrincipal, error) {
	if principal.Name == "" || principal.Name != strings.TrimSpace(principal.Name) ||
		len(principal.Name) > maxACLPrincipalNameBytes || !utf8.ValidString(principal.Name) ||
		strings.ContainsAny(principal.Name, "\x00/\\\r\n") ||
		principal.RequestsPerSecond < 0 || principal.Burst < 0 || principal.MaxConcurrent < 0 {
		return ACLPrincipal{}, ErrInvalidACLPrincipal
	}
	if principal.TokenHash != "" {
		digest, err := hex.DecodeString(principal.TokenHash)
		if err != nil || len(digest) != sha256.Size || strings.ToLower(principal.TokenHash) != principal.TokenHash {
			return ACLPrincipal{}, ErrInvalidACLPrincipal
		}
	}
	if principal.TokenHash == "" && len(principal.CertificateIdentities) == 0 {
		return ACLPrincipal{}, ErrInvalidACLPrincipal
	}
	var err error
	if principal.CertificateIdentities, err = normalizeACLValues(principal.CertificateIdentities); err != nil {
		return ACLPrincipal{}, err
	}
	for _, identity := range principal.CertificateIdentities {
		prefix, value, ok := strings.Cut(identity, ":")
		if !ok || value == "" {
			return ACLPrincipal{}, ErrInvalidACLPrincipal
		}
		switch prefix {
		case "cn", "dns", "uri", "email":
		case "ip":
			if net.ParseIP(value) == nil {
				return ACLPrincipal{}, ErrInvalidACLPrincipal
			}
		default:
			return ACLPrincipal{}, ErrInvalidACLPrincipal
		}
	}
	if principal.Permissions, err = normalizeACLValues(principal.Permissions); err != nil {
		return ACLPrincipal{}, err
	}
	if principal.Tenants, err = normalizeACLValues(principal.Tenants); err != nil {
		return ACLPrincipal{}, err
	}
	return principal, nil
}

func normalizeACLValues(values []string) ([]string, error) {
	if len(values) > maxACLPolicyValues {
		return nil, ErrInvalidACLPrincipal
	}
	out := append([]string(nil), values...)
	for _, value := range out {
		if value == "" || value != strings.TrimSpace(value) || len(value) > maxACLPolicyValueBytes ||
			!utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
			return nil, ErrInvalidACLPrincipal
		}
	}
	sort.Strings(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, ErrInvalidACLPrincipal
		}
	}
	return out, nil
}

func cloneACLPrincipal(principal *ACLPrincipal) ACLPrincipal {
	copyPrincipal := *principal
	copyPrincipal.CertificateIdentities = append([]string(nil), principal.CertificateIdentities...)
	copyPrincipal.Permissions = append([]string(nil), principal.Permissions...)
	copyPrincipal.Tenants = append([]string(nil), principal.Tenants...)
	return copyPrincipal
}

// ACLPrincipalActive reports whether a replicated identity is currently valid.
func ACLPrincipalActive(principal ACLPrincipal, now time.Time) bool {
	return !principal.Revoked && (principal.ExpiresAt.IsZero() || now.Before(principal.ExpiresAt))
}
