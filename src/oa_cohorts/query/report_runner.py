from __future__ import annotations
import pandas as pd
from pydantic import BaseModel, TypeAdapter
from typing import Optional, Callable, Mapping, Any
from typing_extensions import TypedDict
from .report import Report, ReportCohortMap
from .indicator import Indicator

class IndicatorPivotFrameRow(TypedDict):
    person_id: int
    measure_resolver: int
    numerator_date: str
    numerator_measure_id: int
    denominator_date: str
    denominator_measure_id: int
    indicator: int

class ReportMeasurePayload(BaseModel):
    measure_id: int
    materialised_measure_id: Optional[int] = None
    refresh_date: Optional[str] = None

class ReportPayload(BaseModel):
    report_name: str
    report_short_name: str
    report_description: str
    indicators: list[Mapping[str, object]]
    report_cohorts: list[Mapping[str, object]]   
    report_measures: list[ReportMeasurePayload]

class CohortPivotRow(BaseModel):
    person_id: int
    measure_date: str
    cohort_label: str
    subcohort_label: str
    measure_id: int

class IndicatorPivotRow(BaseModel):
    person_id: int
    numerator_measure_id: int
    denominator_measure_id: int
    indicator: int
    numerator_date: str | None = None
    denominator_date: str | None = None

class ReportExportPayload(BaseModel):
    report: ReportPayload
    cohort_demography: list[Mapping[str, object]]
    pivot_cohort: list[CohortPivotRow]
    pivot_indicators: list[IndicatorPivotRow]


def _build_indicator_pivot(report: Report) -> pd.DataFrame:
    
    num_col_map = {
        'measure_date': 'numerator_date',
        'measure_id': 'numerator_measure_id',
        'measure_value': 'numerator_value',
    }

    den_col_map = {
        'measure_date': 'denominator_date',
        'measure_id': 'denominator_measure_id',
        'measure_value': 'denominator_value',
    }

    all_results: list[pd.DataFrame] = []

    for indicator in report.indicators:
        d = indicator.denominator_measure
        n = indicator.numerator_measure

        num = (
            pd.DataFrame(
                n.members,
                columns=['person_id', 'episode_id', 'measure_resolver', 'measure_date'],
            )
            .rename(columns=num_col_map)
            .drop(columns=['episode_id'])
        )

        if d.id != 0:
            denom = (
                pd.DataFrame(
                    d.members,
                    columns=['person_id', 'episode_id', 'measure_resolver', 'measure_date'],
                )
                .rename(columns=den_col_map)
                .drop(columns=['episode_id'])
            )

            num_resolved = (num["person_id"] == num["measure_resolver"]).all()
            denom_resolved = (denom["person_id"] == denom["measure_resolver"]).all()

            if not num_resolved and not denom_resolved:
                denom = denom.assign(measure_resolver=denom["person_id"])
            elif num_resolved and denom_resolved:
                num = num.assign(measure_resolver=num["person_id"])

            indicator_members = denom.merge(num, how="left", on="measure_resolver")
        else:
            indicator_members = num.copy()

        indicator_members = indicator_members.assign(
            numerator_measure_id=n.id,
            denominator_measure_id=d.id,
            indicator=indicator.id,
        )

        if "numerator_date" in indicator_members.columns:
            indicator_members["numerator_date"] = (
                pd.to_datetime(indicator_members["numerator_date"].fillna("2099-12-31"))
                .dt.date
                .astype(str)
            )

        all_results.append(indicator_members)

    if not all_results:
        return pd.DataFrame(
            columns=[
                "person_id",
                "measure_resolver",
                "numerator_date",
                "numerator_measure_id",
                "denominator_date",
                "denominator_measure_id",
                "indicator",
            ]
        )

    df = pd.concat(all_results, ignore_index=True)

    required_cols = {
        "person_id",
        "numerator_measure_id",
        "denominator_measure_id",
        "indicator",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Indicator pivot missing required columns: {missing}")

    return df

def _build_cohort_pivot(report: Report) -> pd.DataFrame:
    import pandas as pd

    cohort_metadata = [
        {
            'cohort_label': c.cohort.dash_cohort_name,
            'subcohorts': [
                {
                    'subcohort_label': cd.dash_cohort_def_name,
                    'measure_id': cd.measure_id,
                    'people': [
                        m._mapping for m in cd.dash_cohort_measure.members
                    ]
                }
                for cd in c.cohort.definitions
            ]
        }
        for c in report.cohorts
    ]

    cohort_data = pd.json_normalize(
        cohort_metadata,
        ['subcohorts', 'people'],
        ['cohort_label', ['subcohorts', 'subcohort_label'], ['subcohorts', 'measure_id']]
    )

    cohort_data = cohort_data.rename(
        columns={c: c.split(':')[-1] for c in cohort_data.columns}
    )

    cohort_data["measure_date"] = (
        pd.to_datetime(cohort_data["measure_date"])
        .dt.date
        .astype(str)
    )

    return cohort_data

def build_report_payload(
    report: Report,
    *,
    demog: pd.DataFrame,
    report_people: pd.DataFrame,
    ind_dict: Callable[[Indicator], Mapping[str, object]],
    cht_dict: Callable[[ReportCohortMap], Mapping[str, object]],
    to_demog_row: Callable[[pd.Series], Mapping[str, object]],
) -> ReportExportPayload:
    """
    Build the full JSON payload for a Report, including:
      - report metadata
      - cohort pivot
      - indicator pivot
      - demography
    """
    report_data = ReportPayload(
        report_name=report.report_name,
        report_short_name=report.report_short_name,
        report_description=report.report_description,
        indicators=[ind_dict(i) for i in report.indicators],
        report_cohorts=[cht_dict(c) for c in report.cohorts],
        report_measures=[
            ReportMeasurePayload(measure_id=m.id)
            for m in report.report_measures
        ],
    )

    cohort_df = _build_cohort_pivot(report)
    indicator_df = _build_indicator_pivot(report)

    pivot_indicators_dicts = indicator_df.to_dict(orient="records")
    pivot_indicators = TypeAdapter(list[IndicatorPivotRow]).validate_python(pivot_indicators_dicts)

    pivot_cohort_dicts = cohort_df.to_dict(orient="records")
    pivot_cohort = TypeAdapter(list[CohortPivotRow]).validate_python(pivot_cohort_dicts)

    demog_rows = TypeAdapter(list[Mapping[str, object]]).validate_python(
        [v for v in demog.merge(report_people).apply(to_demog_row, axis=1)]
    )

    payload = ReportExportPayload(
        report=report_data,
        cohort_demography=demog_rows,
        pivot_cohort=pivot_cohort,
        pivot_indicators=pivot_indicators,
    )

    return payload
