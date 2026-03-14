# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅ Yes     |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report security issues privately via GitHub's
[Security Advisory](https://github.com/youruser/recua/security/advisories/new)
feature (Security → Report a vulnerability).

Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Any suggested mitigations if known

You can expect an acknowledgement within 48 hours and a resolution
timeline within 7 days for critical issues.

## Scope

Security issues in recua itself (e.g. path traversal in `dest`, unsafe
deserialization, credential leakage) are in scope.

Issues in dependencies (`requests`, `rich`, etc.) should be reported
to those projects directly.
