# Atomic Rule Types (`QueryRule`)

`QueryRule` is the smallest declarative unit in the engine.

Each rule:

* targets a field resolved by a `Subquery`
* produces a SQL `WHERE` clause fragment
* participates in rule-level `UNION ALL` composition inside the subquery

The sections below describe the supported matcher types in terms of current code behavior.

---

## 1. `ExactRule`

### Semantics

Match rows whose concept-like field equals a single OMOP concept id.

### SQL shape

```sql
field = concept_id
```

### Typical use

Precise coded events such as a single diagnosis, procedure, or referral concept.

## 2. `HierarchyRule`

### Semantics

Expand one OMOP concept into its descendants using `concept_ancestor`, then match any descendant concept.

### SQL shape

```sql
field IN (descendant_concept_ids)
```

### Typical use

Broad diagnosis or procedure groupings such as stage families or disease branches.

## 3. `HierarchyExclusionRule`

### Semantics

Exclude a concept and all of its descendants.

### SQL shape

```sql
field NOT IN (descendant_concept_ids)
```

### Typical use

Definitions like "all lung cancers except squamous cell carcinoma".

## 4. `PresenceRule`

### Semantics

Match rows where the resolved field is non-null.

### SQL shape

```sql
field IS NOT NULL
```

### Typical use

Existence checks such as:

* any chemotherapy record
* any death date
* any observation in a target domain

## 5. `AbsenceRule`

### Semantics

Match rows where the resolved field is null.

### SQL shape

```sql
field IS NULL
```

### Typical use

Negative cohort definitions such as "no surgery" or "no documented metastases".

## 6. `ScalarRule`

### Semantics

Apply a threshold comparison to a measurable's numeric value column.

Supported threshold directions:

* `>`
* `<`
* `=`
* `!=`

Scalar rules also carry a `threshold_comparator` target, which tells the engine which measurable exposes the numeric column to compare against.

### Concept filtering behavior

Scalar rules have two modes:

* `concept_id = 0`: threshold-only comparison, no concept restriction
* `concept_id != 0`: threshold comparison constrained to a concept-like field

This is an important current usage detail:

* numeric-only measurables are valid for threshold-only scalar rules
* concept-constrained scalar rules require the target measurable to expose both `value_numeric_attr` and `value_concept_attr`

### SQL shape

Threshold-only scalar rule:

```sql
numeric_value_column < threshold
```

Concept-constrained scalar rule:

```sql
field = concept_id
AND numeric_value_column < threshold
```

### Typical use

* treatment within 30 days of death
* diagnosis to treatment interval
* referral-to-specialist interval
* measurement thresholds such as ECOG, lab values, or scores

## 7. `PredicateRule`

### Semantics

Match rows using a boolean predicate column exposed by the measurable.

Predicate rules use `concept_id` as a lightweight boolean flag rather than as an OMOP concept lookup:

* `concept_id is None`: predicate must be `TRUE`
* `concept_id = 1`: predicate must be `TRUE`
* `concept_id = 0`: predicate must be `FALSE`
* any other non-zero value is coerced to `TRUE`

### SQL shape

```sql
predicate_field IS TRUE
```

or

```sql
predicate_field IS FALSE
```

### Typical use

Derived boolean modifiers such as:

* treatment includes radiotherapy
* treatment includes systemic therapy
* concurrent chemo-radiotherapy flag

## 8. `PhenotypeRule`

### Semantics

Expand a phenotype definition into a curated set of concepts and match any of them.

### SQL shape

```sql
field IN (phenotype_concept_ids)
```

### Typical use

Curated research or histology-driven definitions that do not map cleanly onto a single hierarchy.

## 9. `SubstringRule`

### Semantics

Match rows whose resolved string field contains the source concept's `concept_code`.

In practice this is usually used against measurable string fields that store code- or label-like values.

### SQL shape

```sql
field ILIKE '%concept_code%'
```

### Typical use

Fallback matching for legacy coding systems or structured code prefixes where OMOP hierarchy expansion is insufficient.

---

## Summary

| Rule type | Primary measurable field | Typical purpose |
|---|---|---|
| `ExactRule` | concept | precise coded match |
| `HierarchyRule` | concept | inclusive concept family |
| `HierarchyExclusionRule` | concept | exclusion family |
| `PresenceRule` | concept-like field | any recorded value |
| `AbsenceRule` | concept-like field | explicit absence |
| `ScalarRule` | numeric, optionally concept | thresholds and time windows |
| `PredicateRule` | predicate | derived booleans |
| `PhenotypeRule` | concept | curated concept-set expansion |
| `SubstringRule` | string | code/label substring match |
