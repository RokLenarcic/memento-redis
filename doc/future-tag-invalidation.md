# Redis Tag Invalidation Design

**Status:** Implemented.

This document describes the current memento-redis tag invalidation model.

## Summary

Redis tag invalidation relies on two mechanisms:

| Mechanism | Purpose |
| --- | --- |
| Secondary indexes | Remove already-stored tagged entries during invalidation. |
| Redis tag epochs | Reject tagged loads that started before an invalidation but finish after it. |

Stored tagged entries are not revalidated on read. If a tagged value is present in Redis, readers serve it. The invariant is that tag invalidation removes already-indexed entries atomically, and in-flight loaders that were not yet indexed are rejected at completion time.

## Redis Keys

- `epoch_key`: cache-wide Redis string incremented by `sec-index-invalidate.lua` on every tag invalidation for that cache.
- `tag_epochs_key`: Redis hash mapping each secondary-index tag key to the `epoch_key` value observed when that tag was last invalidated.
- `indexes_key`: Redis set containing secondary-index set keys known for a cache/key generator.
- `id_key`: Redis set for one tag identifier, containing entry keys associated with that tag.

Redis stores numbers as strings. Lua receives `INCR` results as numbers and `GET`/`HGET` results as strings, so scripts convert read values with `tonumber` when comparing epochs.

## Entry Value Protocol

Every value stored under a cache entry key starts with one discriminator byte:

| Byte | Shape | Layout |
| --- | --- | --- |
| `0x01` | Untagged value | `[0x01][nippy(value)]` |
| `0x02` | Tagged value | `[0x02][nippy(tagIdents)][nippy(value)]` |
| `0x03` | Load marker | `[0x03][16 bytes UUID]` |

`EntryMeta` is not stored directly. `EntryEnvelope.writeEnvelope` unwraps it before serialization:

- untagged values store just the user value;
- tagged values store tag identifiers and the user value;
- no-cache values are not stored.

Redis tagged envelopes do not serialize a write epoch. Redis epochs are used only for in-flight load completion checks.

## Load Markers

Load markers prevent multiple processes from publishing different values for the same miss.

- `fetch.lua` installs `[0x03][uuid-bytes]` with a short TTL when a loader claims a missing key.
- Other callers seeing a load marker wait through the local/cross-JVM maintenance path rather than running the user function.
- `finish-load.lua` and `finish-load-w-sec.lua` only publish if the marker under the entry key still matches the loader's marker.
- `abandon-load.lua` deletes the marker only if it still matches the loader's marker.

Lua scripts classify load markers by checking byte 1 for `0x03`. Java uses `Load.isLoadMarker` for the full byte-array predicate.

## Invalidation Script

`sec-index-invalidate.lua` is authoritative for already-stored tagged values:

1. Increment `epoch_key`.
2. For each invalidated `id_key`, write the new epoch into `tag_epochs_key`.
3. Read all entry keys from that secondary-index set.
4. Delete those entries.
5. Delete the secondary-index set.
6. Remove the secondary-index set from `indexes_key`.

Redis executes the script atomically. No other Redis command observes a partially-cleaned index. Therefore, an already-stored tagged entry that was indexed before invalidation should be gone after invalidation completes.

## In-Flight Load Rejection

Secondary-index deletion cannot remove a value that has not been written yet. Tag epochs exist to catch that race.

Flow:

1. `fetch.lua` claims a missing key with a load marker and returns the current `epoch_key` as `validation_epoch`.
2. The user function runs and eventually returns a tagged value.
3. `finish-load-w-sec.lua` checks each returned tag's epoch in `tag_epochs_key`.
4. If any `tag_epoch > validation_epoch`, the load is stale.
5. The script deletes the load marker and returns `-1`.
6. `Loader.get` rejects the promise result so local joiners re-loop and reload.

This is the only Redis epoch comparison on the tagged value path.

## Read Path

`Loader.get` and `Loader.ifCached` read cached values directly:

- `fetch.lua` returns either a raw value envelope, a load marker, or a miss.
- Java dispatches by the first byte.
- `0x01` and `0x02` envelopes are decoded with `EntryEnvelope.readEnvelope`.
- `0x03` is treated as an in-flight foreign load.
- Unknown discriminators are deleted defensively by `get`/shared dispatch to avoid livelock.

Reads do not compare `currentEpoch` with a stored `writeEpoch`, because stored tagged entries no longer carry write epochs. Already-written stale tagged entries are handled by secondary-index deletion; in-flight stale tagged entries are handled by `finish-load-w-sec.lua`.

## ifCached

`ifCached` uses `LoaderSupport.fetchCachedEntry`, an observation-only wrapper around `fetch.lua` with `load=0`.

This means:

- a miss never installs a load marker;
- a value hit can refresh fade, matching normal cache hit behavior;
- a load marker returns `EntryMeta.absent` immediately;
- tagged values use the same decode/dispatch path as `get`.

`car/parse-raw` decodes Lua `true` as `1` and Lua `false` as `nil`, so Java checks the first returned element for non-null rather than comparing to `Boolean.TRUE`.

## addEntries / putValue

`RedisCache.addEntries` delegates to `Loader.putEntries`.

`Loader.putEntries`:

- maps user args to Redis entry keys;
- passes raw values to `EntryEnvelope.writeEnvelope`; tagged `EntryMeta` values keep their tag identifiers;
- sends tagged entries one-by-one through `LoaderSupport.putValue`, which writes the value and secondary indexes atomically with `put-value-w-sec.lua`;
- groups untagged entries by expiry and writes them in batches via `LoaderSupport.putValues` and `bulk-set.lua`.

`put-value-w-sec.lua` does not touch epochs. Explicit tagged writes are immediately indexed, so later invalidations can delete them through secondary indexes.

## Maintenance Path

The maintenance daemon handles local promises waiting on foreign load markers.

- It refreshes this JVM's own load markers when requested.
- It tracks foreign waiters as `[entry-key, Load]` pairs, so a maintenance result can only complete the same `Load` that was observed before the Redis fetch.
- It uses `cached-entries.lua` to check the entry keys owned by foreign loaders.
- `cached-entries.lua` reports each key as still a load marker, absent, or raw value bytes.
- Clojure drops still-in-flight load-marker results, decodes value bytes with `EntryEnvelope.readEnvelope`, and returns `[key, load, delivery]` triples for terminal results.
- Maintenance completes a waiter only if `loads-map.remove(key, load)` succeeds. This identity check prevents a stale maintenance result from completing a newer replacement `Load` for the same key.
- Tagged values are delivered to `SpecialPromise` as `EntryMeta`, so `SpecialPromise.deliver` can inspect tag identifiers before `Loader.await` unwraps the user value.

No read-time tag epoch check happens in maintenance; the same stored-entry invariant applies.

## Why Local TagInvalidation Still Exists

The JVM-local `TagInvalidation` model still protects in-process timing windows:

- same-JVM invalidations can reject `SpecialPromise.deliver` before Redis completion;
- same-JVM sibling caches can react to tag invalidations;
- Redis pubsub bridges invalidation start/end events into other JVMs' local invalidation clocks.

Redis tag epochs are the cross-process authoritative guard for stale in-flight tagged load completion. Secondary indexes are the authoritative guard for already-stored tagged entries.
