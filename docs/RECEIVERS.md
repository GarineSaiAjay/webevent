# Writing a webhook receiver

A correct receiver does four things.

1. **Verify the signature on the raw body.** Read the body bytes _before_
   parsing JSON — re-serialised JSON will not match the signature.

```go
   v, _ := webhook.NewVerifier(os.Getenv("WEBHOOK_SECRET")) // github.com/GarineSaiAjay/webevent/pkg/webhook
   body, _ := io.ReadAll(r.Body)
   if err := v.Verify(r.Header, body); err != nil {
       http.Error(w, "invalid signature", http.StatusUnauthorized)
       return
   }
```

Other languages: compute `base64(HMAC_SHA256(key, id + "." + timestamp + "." + body))`
where `key` is the base64-decoded part of `whsec_…`, and compare it in
constant time against each `v1,…` entry of `webhook-signature`. Reject
timestamps more than 5 minutes from your clock. Any Standard Webhooks
library does exactly this.

2. **Deduplicate on `webhook-id`.** Delivery is at-least-once. Store processed
   ids (with a unique constraint) and acknowledge duplicates without
   reprocessing them.

3. **Acknowledge fast, process later.** Return `2xx` within a few seconds and
   do slow work asynchronously. Requests that exceed the delivery timeout
   (15s by default) are treated as failures and retried.

4. **Rotate secrets without downtime.** When a secret is rotated, both the old
   and the new secret sign requests during the grace period. Deploy the new
   secret (you may accept both meanwhile), then let the grace period lapse.

Status codes: `2xx` = done; `410 Gone` = stop sending this delivery;
anything else = retry later (honouring `Retry-After` on `429`/`503`).

`examples/receiver` is a complete, runnable reference implementation.
