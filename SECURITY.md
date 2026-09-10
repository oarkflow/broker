# Security Policy

## Supported versions

The project is currently pre-v1. Until a stable release is published, only the
latest commit on the default branch receives security fixes. No current build
should be treated as a supported production release.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability reporting feature for this repository and include affected
versions, impact, reproduction steps, and any proposed mitigation.

Maintainers should acknowledge a complete report within three business days,
provide a status update within seven business days, and coordinate disclosure
after a fix is available. These are response targets, not a warranty.

## Security expectations

- Production deployments must use authentication and TLS or mTLS.
- `--insecure-dev` is only for isolated development environments.
- Secrets must be supplied through protected environment or file-based secret
  mechanisms, not committed configuration or command-line history.
- A stable release is blocked by unresolved critical or high-severity findings.
