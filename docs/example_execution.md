# Example Execution

This page sketches the current execution flow at a high level. It is intentionally lightweight and reflects the public objects that are exercised in the codebase today.

## 1. Import configuration

The engine expects reports, cohorts, measures, subqueries, and rules to exist in the database. In this repo that is typically loaded from the CSV files under `dash_config/`.

Typical CLI flow:

```bash
oa-cohorts import-config dash_config
```

## 2. Load a report

Once configuration is loaded, a report can be fetched through the ORM and executed against a SQLAlchemy session.

```python
from sqlalchemy import select

from oa_cohorts.query.measure import MeasureExecutor
from oa_cohorts.query.report import Report

report = session.execute(
    select(Report).where(Report.report_short_name == "YOUR_REPORT")
).scalars().unique().one()

executor = MeasureExecutor(session)
report.execute(executor)
report.assert_executed()
```

## 3. Access resolved cohort members

After execution, report cohort membership is available as `MeasureMember` rows.

```python
members = report.members(executor)
```

Each member preserves:

* `person_id`
* `episode_id`
* `measure_resolver`
* `measure_date`

## 4. Build indicator output

Indicators resolve numerator and denominator measures independently, then join them back to the report cohort during output assembly.

In the current codebase this is handled by the reporting/output layer rather than by the indicator object itself emitting a final flat table.

## 5. Inspect SQL and structure

Most query objects are also `HTMLRenderable`, which makes notebook debugging practical:

```python
report
measure
subquery
query_rule
```

These renderers expose:

* structural metadata
* SQL previews where compilation succeeds
* executability status and failure messages where it does not

## Notes on current usage

* Measures in the shipped dashboard config are primarily composed with `OR` and `AND`.
* Threshold-only scalar rules use `concept_id = 0` and can target numeric-only measurables.
* Indicator-level relative date windows are applied during payload assembly, anchored to report cohort membership dates.
