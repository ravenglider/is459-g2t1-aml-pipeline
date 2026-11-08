# Reader-pause coordination and final Role 5 message

Preparation only. Neither Sonia nor Xiang Ying has confirmed a window here.
The user will send the requests in team chat; no messages were sent by this tool.
No publication, reader release or paid query is authorised by this document.

## Send in the team chat

> Hi Sonia and Xiang Ying! We’re preparing full HI/LI sequence publication and
> acceptance, HI first. Could you both explicitly confirm an agreed maintenance
> window before any production publication?
>
> Sonia: please confirm the shared pipeline, scheduled refreshes and other
> production writers/readers touching `bq2_sequences` will be paused during
> publication, recovery if needed, and the agreed acceptance period. Please
> confirm who controls the pause and when publication may start.
>
> Xiang Ying: please confirm Role 6 Athena/CTAS jobs and Tableau/dashboard
> refreshes reading `bq2_sequences` will be paused, and stay paused until the
> applicable publication has passed acceptance and we explicitly agree release.
>
> Proposed window: [start–end, Asia/Singapore]. Please each reply with your
> confirmation, the window and anything still running. If the window expires
> or a run fails, we’ll keep publication/recovery coordinated and reconfirm
> before proceeding. No shared changes will be made by the validation workstream.

If a short separate query window is needed before publication:

> Could we also approve read-only Athena acceptance checks in workgroup [name],
> with [scan budget / query timeout], against the frozen source and completed
> publication? Please confirm the query window and source/output references.
> This is separate from permission to run generation or publish data.

## Record actual replies (do not prefill as approved)

| Gate | Status | Confirmation reference / scope / time |
|---|---|---|
| Sonia: pipeline and other production activity paused | PENDING | [actual message link/text and time] |
| Xiang Ying: Role 6 queries and refreshes paused | PENDING | [actual message link/text and time] |
| Maintenance start/end and pause owner agreed | PENDING | [timezone, UTC equivalents, owners] |
| Generation owner acknowledges both confirmations | PENDING | [actual reference] |
| Paid read-only query execution approved | PENDING | [workgroup, budget, timeout, window] |
| Frozen source and stable output version agreed | PENDING | [run/source fingerprints] |
| Reader release after acceptance agreed | PENDING | [actual owner acknowledgment] |

The generation owner, not the validator, performs publication. Missing, expired,
conditional or ambiguous confirmations do not authorise it. Do not assume
silence means approval. The table-wide pause also applies while the other
dataset is publishing; HI and LI must not publish concurrently. Keep readers
paused on partial Catalog switches or RECOVERY_REQUIRED and coordinate recovery.
Because the integrated full-generation job publishes automatically, obtain both
confirmations before starting that publication-enabled job, not while waiting
for its generation stage to finish.

## Final handoff message (fill only from actual evidence)

> Hi Sonia and Xiang Ying, Role 5 [HI / LI / both] acceptance update:
>
> Glue job: `[exact job name; expected is459-g2t1-bq2-sequence-builder]`.
> HI Glue run: `[JobRunId]`, application run/attempt: `[IDs]`.
> LI Glue run: `[JobRunId / NOT YET AVAILABLE]`, application run/attempt: `[IDs]`.
> Release: `[commit, script URI/hash, module ZIP URI/hash, SDK bundle reference]`.
> Workers/runtime: `[actual Glue version, worker type/count, execution duration,
> DPUSeconds if available; keep validation time separate]`.
>
> Catalog table: `is459_g2t1_aml.bq2_sequences`, approved 21-column v1 schema.
> Selected immutable HI/LI S3 versions: `[exact run/attempt partition roots]`.
> Generation manifests: `[HI URI]`, `[LI URI / pending]`.
> Verified PUBLISHED receipts: `[HI URI]`, `[LI URI / pending]`.
> Three source fingerprints and complete inventories: `[evidence references]`.
>
> Source reconciliation: HI `[actual]/[expected 31,898,218]` rows,
> `[actual]/[expected 35,230]` positive labels; LI `[actual]/[expected 31,251,469]`
> rows, `[actual]/[expected 16,041]` positive labels. These are transaction
> denominators, not sequence counts. Source windows are September 1–28 for HI
> and September 1–27 for LI, with bounded future continuation on the final two dates.
>
> Output counts: HI `[pairs] / [triples]`; LI `[pairs] / [triples or pending]`.
> Owner-date coverage: HI `[28/28 or actual]`; LI `[27/27 or actual/pending]`.
> Validation: `[ACCEPTED / INCOMPLETE / FAILED for each dataset]`.
> Schema/IDs/labels/timing/lineage: `[actual check outcomes]`.
> Athena query IDs and complete date-chunk reconciliation: `[evidence link]`.
> Exceptions or missing checks: `[none only if verified / exact remaining items]`.
>
> Publication/reader-pause confirmations: `[Sonia reference]`, `[Xiang Ying reference]`.
> Reader release: `[explicit agreement / STILL PAUSED]`.
> Xiang Ying: please use the accepted source denominators and original final
> labels, separate pairs/triples, and freeze HI patterns before LI evaluation.
> Sonia: use the accepted publication/run references for later integration;
> do not treat staging-only or probe success as production acceptance.

Until filled, this is a template, not an announcement that outputs are accepted.
