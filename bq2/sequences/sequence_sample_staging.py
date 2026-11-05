"""Explicitly gated bounded sample staging; no publication or runtime creation.

Importing is inert. Injected dependencies are trusted capabilities. A VALIDATED
attempt certifies its Parquet, not successful generation-context exit or source
snapshot completeness. Failures preserve artifacts; retries need fresh IDs.
"""

from copy import deepcopy
from dataclasses import dataclass


@dataclass(frozen=True)
class StagedSampleResult:
    attempt_uri: str
    report: dict


def stage_bounded_sample(config, attempt_id, max_records_per_file, *,
                         allow_writes=False, destination="development",
                         glue_context, catalog_client, s3_client, identity_client,
                         transport=None, _synthetic_baseline=None):
    """Stage inside Phase A; return only after its final checks and cleanup.

    No CLI, manifest readiness, Catalog mutation or publication. The internal
    synthetic hook is forwarded to Phase A's active-local-master gate; it is not
    an AWS argument. transport exists for offline local Parquet tests, like the
    underlying adapter; do not expose it as user-controlled deployment config.
    """
    if allow_writes is not True:
        raise ValueError("Explicit boolean True write activation required")
    from sequence_sample_generation import validate_config, generate_sample_sequences
    from sequence_manifest import _token
    from sequence_s3 import STAGING, _attempt, stage_sequence_batch_s3

    config = validate_config(config)
    if destination != "development":
        raise ValueError("Only development staging is allowed")
    _token(attempt_id)
    if type(max_records_per_file) is not int or max_records_per_file <= 0:
        raise ValueError("max_records_per_file must be a positive integer")
    attempt_uri = STAGING + config["run_id"] + "/" + attempt_id + "/"
    _attempt(attempt_uri)
    try:
        with generate_sample_sequences(config, glue_context=glue_context,
                catalog_client=catalog_client, s3_client=s3_client, identity_client=identity_client,
                _synthetic_baseline=_synthetic_baseline) as generated:
            uri = stage_sequence_batch_s3(s3_client, generated.output, config["run_id"], attempt_id,
                dataset="sample", destination="development", start_date="2022-09-01", timezone="UTC",
                timezone_verified=True, timezone_evidence=config["timezone_evidence"],
                expected_counts=generated.report["validation"]["counts"],
                source_fingerprint=config["expected_source"]["source_fingerprint"],
                max_records_per_file=max_records_per_file, transport=transport)
            if uri != attempt_uri:
                raise ValueError("Staging adapter returned unexpected attempt location")
        if generated.report["status"] != "GENERATION_COMPLETE":
            raise ValueError("Generation context did not complete successfully")
    except BaseException as exc:
        exc.add_note("Preserve any artifacts at " + attempt_uri + "; no overall staging success or publication is certified")
        raise
    return StagedSampleResult(attempt_uri, dict(status="STAGED_GENERATION_COMPLETE",
        published=False, run_manifest_ready=False, generation=deepcopy(generated.report)))
