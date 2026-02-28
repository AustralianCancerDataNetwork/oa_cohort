# Atomic Rule Types (`QueryRule`)

`QueryRule` is the fundamental building block of all cohort and indicator logic.

A rule represents a single declarative clinical condition applied to a specific measurable field. They they provide the minimal logical predicates from which subqueries and measures are constructed.

Each rule translates into a SQL `WHERE` clause fragment against a target field defined by a `Measurable`.

At execution time:

* A `Subquery` selects the appropriate field from a measurable.
* Each `QueryRule` generates a boolean filter expression.
* Rule-level selects are combined via `UNION ALL`.

The sections below describe each supported rule type.

----

## 1. `ExactRule` — Exact Concept Match

### Semantics

Matches records whose concept field is exactly equal to a single OMOP concept.

### SQL Shape

```sql
field = concept_id
```

### Example — Palliative Care Referral

* Subquery: Palliative care referral
* Target: obs_concept
* Definition: `<ExactRule id=139 exact concept=4127745>`

This matches the concept “Referral to palliative care service”.

### Example — Lung Surgery

* Subquery: Lung Surgery
* Target: tx_surgical
* Multiple ExactRules are combined within the subquery:
    * Lobectomy
    * Operation on lung
    * Lung excision
    * Total pneumonectomy

Each rule contributes its own `SELECT`; the subquery performs a `UNION`, preserving all qualifying surgical events.

## 2. HierarchyRule — Inclusive Hierarchical Expansion

### Semantics

Matches a concept and all of its descendants using the OMOP `concept_ancestor` table.

### SQL Shape

```sql
field IN (descendant_concept_ids)
```

### Example — Stage 4 Disease

* Subquery: Stage 4
* Target: dx_stage
* Definition: `<HierarchyRule id=45 hierarchy concept=1633987>`

This expands Stage 4 to include all sub-classifications beneath the parent concept.


## 3. HierarchyExclusionRule — Hierarchical Exclusion

### Semantics

Excludes a concept and all of its descendants. Used to express definitions such as “all X except Y”.

### SQL Shape

```sql
field NOT IN (descendant_concept_ids)
```

### Example — Non-Squamous Disease

* Subquery: Non-squamous disease
* Target: dx_primary
* Definition: `<HierarchyExclusionRule id=410 hierarchyexclusion concept=4300118>`

This excludes squamous cell carcinoma and all its descendants.

## 4. PresenceRule — Any Recorded Value

### Semantics

Matches records where the target field is non-null. Used when the question is simply whether any value exists.

### SQL Shape

```sql
field IS NOT NULL
```

### Example — Any Systemic Therapy

* Subquery: Any Systemic Therapy
* Target: tx_chemotherapy
* Definition: `<PresenceRule id=114 presence>`

This does not constrain the specific drug — it only requires that some chemotherapy record exists. The definition for what qualifies as systemic therapy is therefore dependent upon the definition within the specific `Measurable` mapper class used as the target. In this example it captures the existence of any drug exposure records that have an explicit link to a treatment episode.

### Example — Death

* Subquery: Death
* Target: demog_death
* Definition: <PresenceRule id=109 presence>

Matches any non-null death date.

## 5. AbsenceRule — Explicit Null

### Semantics

Matches records where the field is null. Used to express negative definitions.

### SQL Shape

```sql
field IS NULL
```
This supports constructs such as “no documented metastases” or “no recorded procedure”.

## 6. ScalarRule — Numeric Threshold Comparison

### Semantics

Applies a numeric comparator to a measurable’s numeric value column.

Supports operators: `>, <, ==, !=`

The rule optionally constrains comparison to a specific concept.

Scalar rules resolve their numeric value column indirectly via the `Measurable` registry, allowing the same rule abstraction to operate across domains.

### SQL Shape

```sql
field = concept_id
AND numeric_value_column < threshold
```

### Example — Treatment Within 30 Days of Death

* Subquery: Treatment within 30 days of death
* Target: tx_to_death_window
* Definition: `<ScalarRule id=409 scalar lt 31 on=tx_to_death_window>`

This applies a temporal window threshold at the `Measurable` level, but can be applied to result values or other numeric fields as well.

## 7. PhenotypeRule — Curated Concept Group Expansion

### Semantics

Expands a phenotype definition into a set of concept IDs.

Unlike hierarchical expansion, phenotype sets are curated and may not correspond to OMOP hierarchy structure.

For very complex and deeply layered definitions, this will typically outperform many layers of combinatorial measures required to execute. This ends up being a tradeoff between a more maintainable measure definition relative to vocabulary updates, versus overly deep nesting that can struggle with large cohorts. 

### SQL Shape

```sql
field IN (phenotype_concept_ids)
```

### Example - Non-Small Cell Lung Cancer Phenotype

* Subquery: Non small cell lung cancer phenotype
* Target: dx_primary
* Definition: `<PhenotypeRule id=1607 phenotype phenotype=nsclc>`

Here, the rule does not reference a single concept. Instead, it expands the nsclc phenotype definition into a curated set of concept IDs representing non-small cell lung cancer across coding systems and subtypes.

This is particularly useful where histology-driven definitions often do not align cleanly with a single vocabulary branch.

## 8. SubstringRule — Concept Code Substring Match

### Semantics

Matches concept codes using substring logic.

Used primarily when hierarchical relationships are insufficient or when legacy coding systems rely on structured prefixes.

### SQL Shape

```sql
field ILIKE '%substring%'
```

This is typically a fallback strategy rather than preferred design.

### Example - Mesothelioma

* Subquery: Mesothelioma
* Target: dx_primary
* Definition: `[<SubstringRule id=151 substring concept=44499065>, <SubstringRule id=152 substring concept=44499069>, <SubstringRule id=153 substring concept=44499067>, <SubstringRule id=154 substring concept=44499070>]`

Each rule generates its own `SELECT`. The subquery combines them using UNION ALL, preserving all qualifying diagnosis events.


## Summary Rule Type Comparison

| Rule Type                  | Backed By                              | Best Used For                                      | Typical Oncology Use Case                          |
|----------------------------|------------------------------------------|---------------------------------------------------|---------------------------------------------------|
| `ExactRule`                | Single OMOP concept ID                  | Precisely coded clinical events                   | Lobectomy; Referral to palliative care            |
| `HierarchyRule`            | OMOP `concept_ancestor` expansion       | Broad disease or procedure groupings              | Stage 4 disease; Bronchus cancer                  |
| `HierarchyExclusionRule`   | OMOP `concept_ancestor` expansion       | “All except X” definitions                        | Non-squamous lung cancer                          |
| `PresenceRule`             | Field non-null check                    | Existence of any event in domain                  | Any systemic therapy; Death recorded              |
| `AbsenceRule`              | Field null check                        | Negative definitions                              | No surgery; No documented metastases              |
| `ScalarRule`               | Numeric value column from `Measurable`  | Threshold or temporal comparisons                 | Treatment < 30 days from death; ECOG ≥ 2          |
| `PhenotypeRule`            | Curated concept set                     | Composite or research-defined groupings           | NSCLC phenotype                                   |
| `SubstringRule`            | Concept code pattern match              | Legacy or prefix-based grouping                   | Mesothelioma code block grouping                  |