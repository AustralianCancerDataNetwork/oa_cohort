# `Measurable` Abstraction

The `Measurable` layer is the bridge between the generic query engine and the underlying ORM or materialised-view schema.

It tells the engine:

* which ORM class to query for a given `RuleTarget`
* which columns identify the person, episode, and event date
* which optional columns support concept, numeric, string, or predicate-style filtering

The query engine never hard-codes source-table column names directly. It relies on the measurable contract instead.

## Canonical Output Contract

Every measurable must be able to emit rows in the canonical measure-member shape:

| Column | Meaning |
|---|---|
| `person_id` | individual identifier |
| `episode_id` | clinical episode |
| `measure_resolver` | alignment key used for higher-level joins |
| `measure_date` | date on which the measurable event qualifies |

`MeasurableBase.table_selectables()` and `filter_table_dated()` produce this shape for subqueries and measures.

## `MeasurableSpec`

`MeasurableSpec` is the declarative mapping attached to each measurable class.

Required attributes:

* `domain`
* `label`
* `person_id_attr`
* `episode_id_attr`
* `event_date_attr`

Optional value attributes:

* `value_concept_attr`
* `value_numeric_attr`
* `value_string_attr`
* `value_predicate_attr`

Optional control fields:

* `temporality_map`
* `valid_targets`

The spec stores attribute names, not SQLAlchemy columns. Those names are resolved against the concrete class at bind time.

## Binding

When a class subclasses `MeasurableBase` and defines:

```python
__measurable__ = MeasurableSpec(...)
```

the class is automatically bound to a `BoundMeasurableSpec` via `__init_subclass__`.

Binding rules:

* required columns are resolved immediately
* unsupported optional value channels remain `None`
* no fake SQL constants are injected for missing value channels

That last point is intentional. A measurable that does not support concept filtering should expose `value_concept_col = None`, and the query layer should decide whether that is acceptable for the rules being executed.

## Which Rule Types Need Which Columns

The current query engine resolves measurable fields like this:

| Rule style | Required measurable column |
|---|---|
| exact / hierarchy / hierarchy exclusion / presence / absence | `value_concept_attr` |
| substring | `value_string_attr` |
| predicate | `value_predicate_attr` |
| scalar threshold | `value_numeric_attr` |
| scalar threshold with `concept_id != 0` | `value_numeric_attr` and `value_concept_attr` |

### Scalar-only measurables

Derived window measurables can legitimately expose only a numeric column.

Examples in the current codebase:

* `ReferralToSpecialistWindowMeasurable`
* window-style derived thresholds generally driven by `ScalarRule`

These measurables work when the scalar rule is threshold-only, meaning `concept_id = 0`.

They do **not** support concept-constrained scalar filtering. If a scalar rule with `concept_id != 0` targets such a measurable, subquery compilation raises a targeted error explaining that `value_concept_attr` is required for concept filtering.

## Episode Requirement

This project assumes an episode-linked oncology data model.

Why that matters:

* a person can have multiple disease episodes
* diagnosis, treatment, and modifier rows must align to the correct episode
* composite `AND` logic joins on `measure_resolver`, which is usually the episode id

Without an episode-level resolver:

* `AND` logic would collapse to person-level intersection
* cross-episode contamination would become possible
* time-based qualification would be harder to interpret clinically

## Current Measurable Families

The shipped registry currently maps targets into a small set of measurable families:

* diagnosis measurables: diagnosis concepts, staging, metastasis
* event measurables: measurements, procedures, observations
* treatment measurables: surgery, chemotherapy, radiotherapy, intent, derived windows
* person measurables: death-linked person demography rows

See `src/oa_cohorts/measurables/measurable_resolver.py` for the authoritative target-to-class registry.
