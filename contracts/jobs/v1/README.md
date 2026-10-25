# Job context v1

CYRA-972 defines an additive context shared by background-work adapters. This bundle describes
metadata, not a new ingest kind or endpoint. Its offline fixtures are contract examples, not
application delivery or framework compatibility evidence. Version 1 implementation is in progress.
The existing [ingest contract](../../ingest/v1/README.md) continues to own transport/authentication.

## Wire locations and lifecycle

Metric samples put the object at `contexts.job`. Lifecycle logs put it at
`attributes.contexts.job`, with logger `closeyourit.jobs`, level `info` and fixed message
`Background job attempt finished`. Lifecycle logging is explicitly opt-in and defaults off:
Ruby `job_lifecycle_logs`, Python `CeleryIntegration(lifecycle_logs=True)`. One log represents
one observed attempt completion, including an observed retry decision; it is not a second crash
report. Use the existing SDK event identity once for that emission, including retransmissions.
Lifecycle logging does not establish completeness when disabled, sampled, dropped or undelivered.

Required fields are `schema_version:1`, bounded `framework` and `name`, `attempt_outcome` and
nullable `terminal`. Optional fields are `queue`, framework-owned `job_id`, `attempt` (1-based),
`retry_count` (0-based), UTC timestamps `enqueued_at`/`scheduled_at`/`started_at`, nonnegative
`duration_ms`/`queue_wait_ms`/`scheduled_delay_ms`, `clock_skew`, and metric `measurement`
(`duration` or `queue_wait`). Unknown values are omitted or null; zero means an observed zero.
Attempt and retry count describe their own framework layer and need not differ by exactly one.
A job ID is application identity, not automatically a valid OTLP trace/span identifier.

Never include arguments, keyword arguments, results, headers or arbitrary attributes. The
allowlist is closed. Job names are framework class/task names, not interpolated user values.
IDs identify framework jobs and remain subject to project authorization and bounded retention.

## Time and outcomes

Duration uses elapsed monotonic time. Queue wait uses wall-clock timestamps from the observed
framework: `max(started_at - max(enqueued_at, scheduled_at), 0)`, expressed in milliseconds.
If schedule is unavailable/invalid, omit it and use enqueue as the due time; if enqueue or start
is unknown, queue wait is unknown. Intentional delay/backoff is separately
`max(scheduled_at - enqueued_at, 0)`. Its absence is unknown, not an invented zero.
Timestamps are UTC RFC3339 ending in `Z`; subsecond precision is preserved. The validator allows
0.011ms rounding tolerance. Cross-host accuracy cannot exceed the underlying wall clocks.
`clock_skew:true` marks negative apparent wait that was clamped; it does not diagnose whether
the clock or an early execution caused that ordering.

Outcomes are `success`, `error`, `retry`, `discarded`, `revoked`, `unknown`. Retry means another
attempt was actually scheduled/recognized and therefore `terminal:false`. Unknown outcome has
`terminal:null`. Error does not imply retry exhaustion: terminal is null when the observed layer
cannot know an external adapter's retry decision. Discard/revoke must come from an actual
framework lifecycle event; never infer them from a missing completion or a generic exception.
Only observed outcomes and exact runtime tuples may enter a compatibility claim.

## Legacy compatibility and grouping

Ruby keeps `kind:performance_issue` with subtypes `slow_job` and `job_queue_latency`; Python
keeps `kind:slow_method` and existing `celery.task.duration.*`/`celery.queue.latency.*` labels.
Existing thresholds and comparison boundaries remain intact. Metric `duration_ms` still carries
the measured value selected by `contexts.job.measurement`, not always execution duration.
The canonical context can retain both execution duration and queue wait when known.

Only valid version 1 contexts use the new grouping dimensions: version, framework, name, queue
and measurement, plus the existing wire kind/subtype boundary. Attempt, job ID, outcome and
timestamps never split groups. Unversioned, malformed or future contexts follow the existing
legacy behavior. Existing mixed historical groups are not automatically split or rewritten.
Thresholded legacy metrics are not a count of all attempts; lifecycle logs are separate signals.

Run `python scripts/verify_jobs_contract.py` from the Docs root. Optional file arguments validate
actual extracted job contexts against the same schema and time rules. Consumer bundles follow
the existing `LOCK.json` and checksum convention after the canonical commit is recorded.
