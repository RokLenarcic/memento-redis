# Future Work: Authoritative Cross-Process Tag Invalidation

## Status

Open design item. Not a defect in current behavior, but a known limitation
of the present pubsub-mediated invalidation model.

## Background — three veto layers

memento-redis currently has three independent mechanisms for preventing a
stale value from being served after a tag invalidation:

| Layer | Where it lives                                              | What it catches                                                                                                            |
| ----- | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| L1    | Redis `tag_epochs_key` + `finish-load-w-sec.lua`            | A loader that captured `validation_epoch = E` and then tries to finish a `set` after some tag's epoch has been bumped past `E`. The script returns `-1`; the loader rejects its own publication. |
| L2    | In-JVM `memento.base.TagInvalidation`                       | An in-flight load whose `deliver(...)` call happens while a same-JVM invalidation is "open" (between `startInvalidation` and `endInvalidation`). `SpecialPromise.deliver` consults `lastInvalidatedEpoch` and refuses to publish. |
| L3    | `listener.clj` pubsub: `[:start epoch items]` / `[:end ...]` | Bridges cross-JVM invalidation events into each JVM's local `TagInvalidation`, so foreign JVMs' in-flight loads also veto themselves. |

L1 is **Redis-authoritative** — it sees the up-to-date epoch the moment any
process's `sec-index-invalidate.lua` runs.

L2 is **in-JVM concurrency** — it is the guard for loaders whose `deliver`
step has not yet happened. It is updated cross-process only via L3.

L3 is **eventually consistent** — pubsub may be delayed, drop messages on
connection blips, or arrive after the local process has already served reads.

## The gap (M1)

`Loader.get` has a "hit" branch (the `fetch.lua` claim found an existing
cached value in Redis):

```java
// there's a cached value, remove the load and return it if valid, otherwise absent
loads.remove(key, newLoad);
cachedVal = markCached(cachedVal);
return p.deliver(cachedVal, latestTagInvalidation(cachedVal)) ? EntryMeta.unwrap(cachedVal) : EntryMeta.absent;
```

`latestTagInvalidation(cachedVal)` consults only L2 — the local in-JVM
`TagInvalidation`. It does **not** read Redis's `tag_epochs_key`. So:

* `fetch.lua` returns `{true, value, 0}` on a hit without reading any tag
  epoch — see `src/memento/redis/fetch.lua`.
* The Java side has no per-entry write epoch stored alongside `value` that
  it could compare a freshly-read `tag_epochs_key[t]` against, even if it
  did read it.
* If Process A invalidates tag `T`, then Process B (with a delayed pubsub)
  serves a hit for a value tagged `T` — Process B has no way to know
  the tag was invalidated. It returns the stale value.

The window is the L3 propagation delay. Under healthy pubsub this is
sub-millisecond; under stress, network blips, or subscriber backpressure
it can be longer, and missed messages effectively make the window
permanent until something else (TTL, segment invalidate) evicts the entry.

## Why L2 still exists despite L1

L2 catches paths that never reach `finish-load-w-sec.lua`:

1. **`EntryMeta.isNoCache()` branch in `Loader.get`** — the loader publishes
   the value via the promise (joiners receive it) and calls `abandonLoad`;
   `completeLoad` is never invoked, so L1 has no chance to veto.
2. **Same-JVM sibling caches** (e.g. a sibling Caffeine memento cache in the
   same JVM sharing tag idents) consume `TagInvalidation` directly. L3 is
   their only signal that an invalidation originating in Redis has begun.
3. **Same-JVM in-flight loads** between `startInvalidation` and
   `completeLoad` are caught by L2 at `deliver` time; L1 would catch the
   subsequent `completeLoad` too, but L2 lets the loader abandon earlier
   (no Redis script round-trip).

So L2 is not redundant with L1 — they cover different sub-paths and L2
also services sibling backends.

## Proposed remediation — store per-entry write epoch and revalidate inline

Make Redis the authority on the hit path too, by:

1. **Capture write epoch on completion.** When a loader's `completeLoad`
   succeeds, the value written into `k` includes the `validation_epoch`
   that was captured at `fetch.lua` time (now known to be safe, since
   `finish-load-w-sec.lua` already verified `tag_epoch <= validation_epoch`
   for every tag).

   Concretely, extend the value envelope. Today `cval` serializes
   `EntryMeta` (or raw value) to bytes via Nippy. Add a `writeEpoch` long
   to the envelope, populated from the `validation-epoch` argument to
   `completeLoad`.

2. **Revalidate inline in `fetch.lua` on hit.** When the script finds an
   existing value, in addition to returning the value it must also return
   either (a) the current `epoch_key` value, or (b) the `tag_epochs_key`
   hash entries relevant to this entry's tags.

   Option (a) is cheaper (one `get` already in scope), but requires the
   Java side to do the per-tag lookup separately or trust a coarse
   "any-tag-bumped" signal. Option (b) needs the entry's tag list to be
   known by the script — currently it isn't. The compromise: return
   the current `epoch_key` value as the third element on the hit path
   (instead of the placeholder `0` that the post-H1 fix returns), and
   have the Java side decide whether to do further work.

3. **Compare in Java.** After `fetch.lua` returns `{true, value, current_epoch}`,
   parse the entry's `writeEpoch` from the envelope and its `tagIdents`
   from the `EntryMeta`. If `current_epoch > writeEpoch`, issue an
   `HMGET tag_epochs_key tag1 tag2 ...` to determine whether any of the
   entry's tags was bumped past `writeEpoch`. If yes, treat the hit as a
   miss (return `EntryMeta.absent`, let the caller re-loop and load
   afresh).

   The `current_epoch > writeEpoch` pre-check makes the extra `HMGET` rare
   in the steady state — only after any invalidation has happened since
   this entry was written.

### Trade-offs

* **Envelope change is backwards-incompatible** for cached values written
  by older versions. Either run a migration (touch each value, or just
  let TTLs evict old entries), or include a version byte at the front of
  the envelope and treat versionless entries as having `writeEpoch = -inf`
  (i.e., always revalidate on first read after upgrade).
* **Extra `HMGET` on cold reads after invalidation** — bounded by the
  number of tags on the entry, typically small.
* **Removes pubsub from the critical path for cross-process tag
  invalidation correctness.** Pubsub still serves L2 for in-flight
  same-JVM loads and sibling caches, but a missed pubsub message no
  longer causes stale hits to be served indefinitely.

### Alternative: best-effort revalidate via local epoch counter

A lighter intermediate step: have `fetch.lua` always return the current
`epoch_key` value on hit. Each cache instance tracks its last-seen
`epoch_key`. On observing a bumped epoch, refresh the local
`TagInvalidation` from a fresh `HGETALL tag_epochs_key`.

This closes the pubsub-gap window probabilistically (any read recovers
missed invalidations) without changing the value envelope. It is still
not strictly authoritative — a concurrent invalidate between the
`fetch.lua` return and the `lastInvalidatedEpoch` call can slip
through — but the window shrinks from "pubsub propagation latency" to
"one Java method call." For many deployments this is enough.

## Scope of work for the full fix

* `src/memento/redis/loader.clj` — `cval` / value envelope to include
  `writeEpoch`.
* `src/memento/redis/fetch.lua` — return current `epoch_key` on the hit
  branch; document the three-element contract.
* `java/memento/redis/poll/Loader.java` — parse `writeEpoch`, compare
  against returned current epoch, optionally issue `HMGET` against
  `tag_epochs_key`, treat as miss when stale.
* `java/memento/redis/poll/LoaderSupport.java` — possibly a new method
  for the bulk tag-epoch read.
* Tests: cross-JVM test (or simulated two-listener setup) where pubsub
  is suppressed to verify the hit-path revalidation rejects stale
  values.

## Acceptance criteria

A read on Process B that occurs **after** Process A has completed a tag
invalidation must return either the new (post-invalidation) value or
`absent`, regardless of pubsub state. The current behavior — returning
the pre-invalidation value until pubsub catches up — is the bug.

## Out of scope

* Authoritative invalidation for `EntryMeta.isNoCache()` (the value never
  reaches Redis; L2 + L3 remain the only veto).
* Sibling-cache (e.g. Caffeine) consumption of `TagInvalidation` — that
  continues to rely on L3.
