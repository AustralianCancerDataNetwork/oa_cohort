# `Measurable` Abstraction

The `Measurable` abstraction is the bridge between the engine and the underlying database schema (e.g. OMOP CDM or oncology-specific materialised views).

It gives the generalised query resolution pointers to the target columns and tables on which to operate.

The `Measurable` layer is the component that understands:

* Which ORM class to query
* Which columns represent person, episode, and date
* Which value columns support numeric / concept / string filters

All measurables must emit rows in the canonical form:

| Column            | Meaning                                         |
|-------------------|-------------------------------------------------|
| `person_id`       | Individual identifier                           |
| `episode_id`      | Clinical episode                                |
| `measure_resolver`| Logical grouping key (usually episode)          |
| `measure_date`    | Date at which criterion was satisfied           |

This shape is required so that:

* Subqueries generalise and therefore can combine rule-level filters
* Measures can apply OR / AND semantics
* Resolver-aligned joins can occur
* Higher-level reporting or visualisation layers can filter by qualification date

### Episode Requirement (Cancer Context)

This implementation enforces a hard requirement that all measurable ORM entities must expose an `episode_id`. This is intentional and oncology-specific.

In cancer:

* A person may have multiple disease episodes
* Treatments and staging events must align to specific episodes
* `AND` logic must join on a resolver representing a clinical context

Without an episode-level resolver:

* `AND` logic would collapse to person-level intersection
* Cross-episode contamination could occur
* Temporal semantics would be incorrect

Resolver alignment therefore depends on episode-level linkage.

## `MeasurableSpec`

`MeasurableSpec` is a declarative mapping from ORM class to the regular shape required to produce measure queries.

It defines:

* Domain (dx, tx, meas, obs, proc, person)
* Attribute names for:
    * person_id
    * episode_id
    * event_date
* Optional value attributes:
    * value_numeric_attr
    * value_concept_attr
    * value_string_attr
* Optional temporality overrides
* Optional valid targets

It references attribute names, not column objects, which allows reuse across different ORM classes.

## Binding Process

When an ORM class subclasses `MeasurableBase` and defines:

```python
__measurable__ = MeasurableSpec(...)
```

The `__init_subclass__` hook automatically binds it to:

```python
__bound_measurable__
```

This resolves attribute names into actual SQLAlchemy column objects, and the result is a `BoundMeasurableSpec` that can be actually used by the subquery logic.

## MeasurableBase Contract

`MeasurableBase` defines the minimal interface required for participation in measure logic.

Key methods:

* `person_id_col()`
* `episode_id_col()`
* `event_date_col()`
* `temporal_anchor(temporality)`
* `filter_table()`
* `filter_table_dated(temporality)`

`filter_table_dated` must return selectables labelled as `[person_id, episode_id, measure_resolver, measure_date]` to match the contract that a `Subquery` can operate against.

# Design Constraint

`oa_cohorts` assumes:

* Episode-linked data model
* Resolver-based AND semantics
* Canonical four-column output shape

It does not currently support:

* Pure person-level resolution
* Resolver-free AND logic
* Schemas without episode linkage

Adapting this engine to non-episode-based domains would require redefining the resolver model at the `Measurable` layer and then using the `person_ep_override` property at the measure level (and cascaded down) to alias the resolver.