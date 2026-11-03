"""Pure date planning for one independently selected transaction dataset.

No Spark session, filesystem access or AWS integration. Dataset bounds must be
confirmed from a complete source inventory/upstream handoff, never inferred from
the last partition currently visible. Bounds describe observed source coverage,
not a claim that real-world activity ended on that date.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable


ONE_DAY = timedelta(days=1)


def _iso_date(value: str, name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a YYYY-MM-DD string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid YYYY-MM-DD date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must use canonical YYYY-MM-DD format")
    return parsed


@dataclass(frozen=True)
class DateBatchPlan:
    """One output owner, plus explicitly classified continuation coverage.

    Dates are datetime.date values, not timestamps; execution must use the
    verified Spark timezone. A ready plan has no unexplained missing partitions.
    It may still have explicitly recorded end-of-source truncation.
    """

    start_date: date
    output_end_exclusive: date
    dataset_start_date: date
    dataset_end_date: date
    required_input_dates: tuple[date, ...]
    available_input_dates: tuple[date, ...]
    verified_empty_input_dates: tuple[date, ...]
    unavailable_after_dataset_end: tuple[date, ...]
    unexpected_missing_input_dates: tuple[date, ...]

    @property
    def ready_to_process(self) -> bool:
        return not self.unexpected_missing_input_dates


def plan_start_date_batches(
    start_date: str,
    end_date: str,
    *,
    dataset_start_date: str,
    dataset_end_date: str,
    available_partition_dates: Iterable[str],
    verified_empty_dates: Iterable[str] = (),
) -> tuple[DateBatchPlan, ...]:
    """Plan inclusive requested start dates, each owning exactly one calendar day.

    Every owner D requires D/D+1/D+2. Classify absent dates beyond the explicitly
    confirmed dataset end as observation-boundary truncation; absent dates inside
    coverage are unexpected unless independently verified empty. Return blocked
    plans for diagnostics; a later runner MUST refuse a non-ready plan.

    The later runner must filter starting pairs to first-transaction date D
    BEFORE triple expansion, while retaining continuation pairs starting on D or
    D+1. Do not truncate all transactions to D or filter triples by total duration.
    This planner establishes partition coverage, not chain validity or source
    snapshot consistency. Existing Spark functions retain exact link predicates.
    """
    first = _iso_date(start_date, "start_date")
    last = _iso_date(end_date, "end_date")
    source_first = _iso_date(dataset_start_date, "dataset_start_date")
    source_last = _iso_date(dataset_end_date, "dataset_end_date")
    if source_first > source_last:
        raise ValueError("Dataset coverage range is reversed")
    if first > last:
        raise ValueError("Requested start-date range is empty/reversed")
    if first < source_first or last > source_last:
        raise ValueError("Requested start dates must lie within confirmed dataset coverage")
    # Needed for both look-ahead planning and a half-open ownership interval.
    if last > date.max - 2 * ONE_DAY:
        raise ValueError("Requested start date cannot accommodate two look-ahead dates")

    def inventory(values, name):
        if isinstance(values, (str, bytes)):
            raise ValueError(f"{name} must be an iterable of date strings, not one string")
        try:
            dates = {_iso_date(value, name) for value in values}
        except TypeError as exc:
            raise ValueError(f"{name} must be an iterable of date strings") from exc
        if any(d < source_first or d > source_last for d in dates):
            raise ValueError(f"{name} contains dates outside confirmed dataset coverage")
        return dates

    available = inventory(available_partition_dates, "available_partition_dates")
    empty = inventory(verified_empty_dates, "verified_empty_dates")
    if available & empty:
        raise ValueError("A date cannot be both an available partition and verified empty")

    plans = []
    owner = first
    while owner <= last:
        required = tuple(owner + offset * ONE_DAY for offset in range(3))
        in_bounds = tuple(d for d in required if d <= source_last)
        plans.append(DateBatchPlan(
            start_date=owner,
            output_end_exclusive=owner + ONE_DAY,
            dataset_start_date=source_first,
            dataset_end_date=source_last,
            required_input_dates=required,
            available_input_dates=tuple(d for d in in_bounds if d in available),
            verified_empty_input_dates=tuple(d for d in in_bounds if d in empty),
            unavailable_after_dataset_end=tuple(d for d in required if d > source_last),
            unexpected_missing_input_dates=tuple(d for d in in_bounds if d not in available | empty),
        ))
        owner += ONE_DAY
    return tuple(plans)
