#OA Cohorts – Reporting & Cohort Execution Engine

This package provides the core machinery for defining, executing, and inspecting cohort-based reports over OMOP-style clinical data. It’s designed to support building real-world evidence reports from composable clinical rules, measures, cohorts, and indicators, with both programmatic APIs and lightweight HTML rendering for debugging and exploration.

At a high level, the system lets you:

* Define query rules (exact, hierarchical, scalar thresholds, phenotypes, etc.)
* Combine rules into subqueries
* Build measures from subqueries (including composite measures with AND/OR/EXCEPT logic)
* Group measures into dash cohorts and cohort definitions
* Define indicators (numerator/denominator pairs)
* Assemble everything into a report
* Execute the report against a database session and materialise results as in-memory member sets
* Inspect SQL, executability, and structure via HTML renderers (handy in notebooks)

This is intentionally object-centric: once a report is executed, all downstream payloads (cohort pivots, indicator pivots, demographics, etc.) are assembled from the object graph rather than re-querying the database.

## What’s here (roughly)

* `Report / ReportCohortMap`: Top-level report definition, linking cohorts and indicators.
* `DashCohort / DashCohortDef`: User-facing cohort groupings backed by executable measures.
* `Measure / MeasureSQLCompiler / MeasureExecutor`: The core executable units. Measures compile to SQL, execute against a session, and materialise member sets with dating and episode context.
* `Indicator`: Numerator/denominator semantics over measures, with temporal constraints.
* `QueryRule (+ subclasses)`: The rule DSL: exact matches, hierarchies, exclusions, scalar thresholds, phenotypes, substring matches, etc.
* `HTMLRenderable mixins`: Lightweight visualisation of structure, SQL previews, and executability for debugging and exploration.

## Execution model 

```python
report.execute(session)
report.assert_executed()

rows = report.members              # all cohort members
indicators = report.indicators     # now have numerator_members / denominator_members
```

### Status

This is a working internal engine under active development. APIs may shift.