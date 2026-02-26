import sqlalchemy as sa
from ..query.typing import SQLQuery
from omop_constructs.alchemy.demography import PersonDemography

class DemographyFilter:
    def __init__(
        self,
        *,
        sex: str | None = None,
        min_age: int | None = None,
        max_age: int | None = None,
        language: str | None = None,
        country_of_birth: str | None = None,
        post_code: int | None = None,
        alive_only: bool | None = None,
        index_date: sa.ColumnElement | None = None,
    ):
        self.sex = sex
        self.min_age = min_age
        self.max_age = max_age
        self.language = language
        self.country_of_birth = country_of_birth
        self.post_code = post_code
        self.alive_only = alive_only
        self.index_date = index_date  

    def _base_stmt(self) -> sa.Select:
        return sa.select(PersonDemography)

    def _apply_filters(self, stmt: sa.Select) -> sa.Select:
        if self.sex:
            stmt = stmt.where(PersonDemography.sex == self.sex)

        if self.language:
            stmt = stmt.where(PersonDemography.language_spoken == self.language)

        if self.country_of_birth:
            stmt = stmt.where(PersonDemography.country_of_birth == self.country_of_birth)

        if self.post_code:
            stmt = stmt.where(PersonDemography.post_code == self.post_code)

        if self.alive_only is True:
            stmt = stmt.where(PersonDemography.death_datetime.is_(None))

        if self.min_age or self.max_age:
            if self.index_date is None:
                raise ValueError("index_date required for age filtering")

            age_expr = sa.func.extract("year", self.index_date) - PersonDemography.year_of_birth

            if self.min_age:
                stmt = stmt.where(age_expr >= self.min_age)
            if self.max_age:
                stmt = stmt.where(age_expr <= self.max_age)

        return stmt

    def to_person_ids_subquery(self) -> sa.Subquery:
        stmt = sa.select(PersonDemography.person_id).distinct()
        stmt = self._apply_filters(stmt)
        return stmt.subquery()

    def to_rows_stmt(self, *, restrict_to_person_ids: list[int] | None = None) -> sa.Select:
        """
        Return a SELECT that yields full demography rows (for payload materialisation).
        """
        stmt = self._base_stmt()
        stmt = self._apply_filters(stmt)

        if restrict_to_person_ids:
            stmt = stmt.where(PersonDemography.person_id.in_(restrict_to_person_ids))

        return stmt