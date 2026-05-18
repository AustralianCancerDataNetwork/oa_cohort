# Measure Resolution 

## MeasureMember - atomic unit of result

When a measure is executed, it produces a sequence of:

```Python
@dataclass(frozen=True)
class MeasureMember:
    person_id: int
    measure_resolver: int
    episode_id: Optional[int]
    measure_date: Optional[date]
```

A `MeasureMember` represents a specific person qualifying for a measure at a specific time, for a specific resolver.

It is not only the fact of membership, because it includes resolution to a time-stamped qualification event. 
This distinction is critical for reporting, which requires flexible time-windowing periods to produce trends and per-period analysis.

## Measure Combination Semantics

Measures use `RuleCombination` to compose child measures via `OR` or `AND`, and the handling of these semantics is significantly different when composing from lower levels.

![Qualification temporal resolution](img/temporal_resolver_1.png)

### OR Logic: Union of qualifying events

![Qualification temporal resolution](img/temporal_resolver_2.png)

`OR` logic preserves all qualifying rows.

Implementation:

* Each child measure emits rows
* Rows are combined using `UNION ALL`
* Multiple qualification dates are preserved.
* No resolver alignment is required.
* Events bubble upward unchanged.

Example:

* Measure = ECOG 0 OR ECOG 1

If a person has:

* ECOG 0 on Jan 1
* ECOG 1 on Mar 1

Result: Two `MeasureMember` rows, with both dates preserved

### AND Logic: Resolver-Aligned Intersection

![Qualification temporal resolution](img/temporal_resolver_3.png)

`AND` logic is not simply “person appears in both”. It requires that the same resolver must satisfy all child criteria.

Implementation:

* Each child measure emits canonical rows.
* Children are joined on measure_resolver.
* Resolver alignment is required.
* Qualification date shifts forward to the last satisfied condition.
* If resolvers differ, the row is excluded.
* Qualification date becomes: `greatest(child_1_date, child_2_date, ...)`

This represents the earliest moment at which all criteria are true.

Example:

* Measure = Stage III AND Radiotherapy

If a person has:

* Stage III on Jan 1 (episode 10)
* Radiotherapy on Feb 15 (episode 10)

Result: One row, qualification date = Feb 15

If RT occurred under episode 20 instead, there is no result (resolvers do not align)

### Temporal Window: Event-to-Event Timing

A third measure kind handles cases where qualification depends on the relationship between two events — specifically, whether event B occurs within a defined window relative to event A.

This is expressed via `MeasureTemporalWindow`, a separate config table with a 1:1 relationship to `Measure` (enforced by primary key). A measure with a `window_config` row is a temporal window measure.

**Anchor and candidate**

The measure's own `subquery_id` defines the anchor event. A separate `candidate_measure_id` points to another measure whose events are evaluated relative to that anchor. The anchor is deduplicated to one row per resolver (earliest event) before the join.

**Window bounds**

`window_min_days` and `window_max_days` are optional. `NULL` means unbounded on that side. Negative values are valid — a candidate event occurring before the anchor still qualifies, and the resulting negative delta is preserved in the output for downstream benchmark comparison.

**Pick strategy** (`window_pick_strategy`)

Controls which candidate event is retained per resolver after applying the window filter:

| Value | Behaviour |
|-------|-----------|
| `any` | All qualifying candidate events are preserved |
| `earliest` | One row per resolver: earliest qualifying candidate date |
| `latest` | One row per resolver: latest qualifying candidate date |
| `closest` | One row per resolver: candidate date closest to anchor date |

Default when `NULL`: `earliest`.

**Result date** (`result_date_source`)

Controls which date is emitted as `measure_date` in the output:

| Value | Emitted date |
|-------|-------------|
| `candidate` | Date of the candidate event |
| `anchor` | Date of the anchor event |
| `greatest` | Later of anchor or candidate |
| `least` | Earlier of anchor or candidate |

Default when `NULL`: `candidate`.

**Resolver alignment** (`require_same_resolver`)

When `TRUE` (default), the anchor and candidate are joined on both `person_id` and `measure_resolver`. When `FALSE`, the join is on `person_id` only.

**SQL variants**

`sql_any()` and `sql_first()` are supported. `sql_undated()` raises `NotImplementedError` — this surfaces as `WARN` (not `FAIL`) in `is_executable()` because the measure is still usable in the normal execution path.

**Configuration**

Temporal window measures are loaded via `measure_temporal_window.csv` in the config directory. The CSV is optional — existing deployments without it are unaffected.

Example — GP referral to treatment within 42 days:

```
Measure:
  name:        GP referral to first treatment or pall care <= 42d
  combination: rule_or
  subquery_id: <GP referral subquery>

MeasureTemporalWindow:
  candidate_measure_id: <any treatment OR pall care composite>
  window_min_days:      NULL
  window_max_days:      42
  window_pick_strategy: earliest
  result_date_source:   candidate
  require_same_resolver: TRUE
```

### Nested Measures

Measures can be nested arbitrarily:

```
A AND (B OR C)
```

Evaluation proceeds bottom-up:

1. B OR C → union of events
2. A → events
3. AND joins A with (B OR C) on resolver
4. Qualification date = greatest of aligned dates

The canonical shape is preserved at every level.