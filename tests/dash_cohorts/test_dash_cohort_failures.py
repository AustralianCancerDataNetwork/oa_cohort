def test_cohort_definition_missing_measure(definition_missing_measure, executor):
    assert definition_missing_measure.members(executor) == ()

def test_cohort_member_deduplication(cohort, executor):
    members = cohort.members(executor)

    ids = [(m.person_id, m.measure_resolver) for m in members]

    assert len(ids) == len(set(ids))