import pytest
from oa_cohorts.query.measure import MeasureExecutor, MeasureSQLCompiler

def test_measure_zero_cannot_execute(db, measure_zero):

    executor = MeasureExecutor(db)

    with pytest.raises(RuntimeError, match="FULL COHORT"):
        executor.execute(measure_zero)

def test_measure_members_without_execution(db, measure_leaf):

    executor = MeasureExecutor(db)

    with pytest.raises(RuntimeError, match="has not been executed"):
        executor.members(measure_leaf)


def test_measure_executor_force_refresh(db, measure_leaf):
    executor = MeasureExecutor(db)

    rows1 = executor.execute(measure_leaf)
    rows2 = executor.execute(measure_leaf, force_refresh=True)

    assert rows1 is not rows2

def test_measure_without_subquery_or_children_fails(measure_empty):

    compiler = MeasureSQLCompiler(measure_empty)

    with pytest.raises(ValueError, match="no subquery and no children"):
        compiler.sql_any()

def test_measure_sql_normalise_missing_columns(measure_with_bad_child):

    compiler = MeasureSQLCompiler(measure_with_bad_child)

    with pytest.raises(ValueError, match="missing columns"):
        compiler.sql_any()

def test_invalid_combination_sql(measure_invalid_combination):

    compiler = MeasureSQLCompiler(measure_invalid_combination)

    with pytest.raises(KeyError):
        compiler.sql_any()

def test_subquery_without_rules_fails(subquery_empty):
    with pytest.raises(ValueError, match="has no rules"):
        subquery_empty.sql_any()

def test_subquery_unknown_target(subquery_unknown_target):
    with pytest.raises(KeyError, match="No measurable registered"):
        subquery_unknown_target.sql_any()

def test_subquery_missing_numeric_column(subquery_scalar_rule):
    with pytest.raises(ValueError, match="does not expose required numeric"):
        subquery_scalar_rule.sql_any()