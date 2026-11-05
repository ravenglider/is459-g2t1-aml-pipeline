"""Offline compatibility gates for a future Glue entry point.

No SDK imports, credentials, network calls, Spark startup or environment reads.
The caller supplies actual Spark/JVM versions; Python uses this interpreter.
Checking a configured Glue target does not independently inspect an AWS job.
Model support is necessary, not proof of S3 permissions or successful CAS writes.
"""

from collections.abc import Mapping
import json
import re
import sys


SPARK_VERSIONS = ("3.5.4", "3.5.4-amzn-0")
CONDITIONAL_PARAMETERS = ("IfNoneMatch", "IfMatch")


class GlueCompatibilityError(ValueError):
    """Safe diagnostics: fixed requirement names only, never client/value dumps."""

    def __init__(self, reason, **details):
        self.diagnostic = dict(status="INCOMPATIBLE", reason=reason, values_logged=False, **details)
        super().__init__(json.dumps(self.diagnostic, sort_keys=True))


def validate_glue_runtime(*, glue_version, spark_version, java_version):
    """Require explicit Glue 5.0, approved Spark, Python 3.11 and Java 17.

    The active Python major/minor is checked directly, not a user-supplied label.
    Version strings are not normalized to admit other Spark vendor builds.
    """
    checks = dict(
        glue=isinstance(glue_version, str) and glue_version == "5.0",
        spark=isinstance(spark_version, str) and spark_version in SPARK_VERSIONS,
        python=sys.version_info[:2] == (3, 11),
        java=isinstance(java_version, str) and len(java_version) <= 128
        and re.fullmatch(r"17(?:\.\d+)*(?:[+_-][A-Za-z0-9._+-]+)?", java_version) is not None,
    )
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise GlueCompatibilityError("unsupported_runtime", failed_requirements=failed)
    return dict(status="COMPATIBLE", checks=checks, glue_version="5.0",
                spark_version=spark_version, python_major_minor=[3, 11], java_major=17)


def validate_s3_conditional_writes(client):
    """Inspect the injected client's local botocore PutObject input model only.

    Never call put_object or infer compatibility from SDK version strings.
    Missing/malformed metadata and non-string conditional shapes fail closed.
    Unrelated model members and original exception messages are never logged.
    """
    try:
        service = client.meta.service_model
        if service.service_name != "s3":
            raise ValueError()
        operation = service.operation_model("PutObject")
        if operation.name != "PutObject":
            raise ValueError()
        shape = operation.input_shape
        if shape.type_name != "structure" or not isinstance(shape.members, Mapping):
            raise ValueError()
        members = shape.members
        missing = [name for name in CONDITIONAL_PARAMETERS if name not in members]
        invalid = [name for name in CONDITIONAL_PARAMETERS
                   if name in members and members[name].type_name != "string"]
    except Exception:
        # Do not expose SDK exceptions: they can include URLs/configuration.
        raise GlueCompatibilityError("malformed_s3_operation_model", operation="PutObject") from None
    if missing or invalid:
        raise GlueCompatibilityError("unsupported_conditional_writes", operation="PutObject",
                                     missing_capabilities=missing, invalid_capabilities=invalid)
    return dict(status="COMPATIBLE", operation="PutObject",
                required_capabilities=list(CONDITIONAL_PARAMETERS), missing_capabilities=[],
                conditional_writes_supported=True)
