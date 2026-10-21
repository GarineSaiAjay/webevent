# Security

## Reporting a vulnerability

Please do not open a public issue. Email the maintainer (see the GitHub
profile) with a description and reproduction steps. You will get an answer
within 72 hours.

## Threat model and mitigations

| Threat                                                        | Mitigation                                                                                                                                                                             |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Forged webhooks sent to receivers                             | HMAC-SHA256 signatures (Standard Webhooks) with timestamps; receivers reject >5 min skew                                                                                               |
| Replay of captured webhooks                                   | signed timestamp + receiver dedupe on `webhook-id`                                                                                                                                     |
| SSRF: endpoints targeting internal services or cloud metadata | destination IP checked in the dialer after DNS resolution (defeats DNS rebinding); private, loopback, link-local, CGNAT, multicast and reserved ranges blocked; no redirects; no proxy |
| Hostile receivers (huge/slow responses)                       | request timeout, response read capped at 64 KiB, stored at 4 KiB                                                                                                                       |
| Database leak exposes signing secrets                         | AES-256-GCM encryption at rest with `WEBEVENT_SECRET_KEY`                                                                                                                              |
| API key brute force / timing attacks                          | keys ≥ 32 chars, constant-time digest comparison                                                                                                                                       |
| Log injection via request ids                                 | `X-Request-Id` accepted only if it matches `[A-Za-z0-9._-]{1,64}`                                                                                                                      |
| Oversized requests                                            | 512 KiB body limit, 256 KiB payload limit, strict JSON decoding                                                                                                                        |
| Exposure of internals                                         | `/metrics` and `/debug/pprof` on a separate admin port that must not be public                                                                                                         |
| Supply chain                                                  | Dependabot, `govulncheck` in CI, distroless non-root image                                                                                                                             |

## Operational guidance

- Never set `WEBEVENT_ALLOW_PRIVATE_NETWORKS=true` in production.
- Terminate TLS in front of the API (ingress / load balancer).
- Keep the admin port reachable only from your monitoring network.
- Rotate API keys and endpoint secrets periodically; both support overlap.
