from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence
from sqlalchemy import Enum
import enum, uuid


class Materialised_Measure(Base): 
    __tablename__ = 'materialised_measure'

    measure_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True)
    id = so.synonym('measure_id')
    
    refresh_date: so.Mapped[date] = so.mapped_column(sa.DateTime, server_default=sa.sql.func.now())

    measure: so.Mapped['Measure'] = so.relationship('Measure', foreign_keys=[measure_id])
    measure_people: so.Mapped[List['Measure_Person_Map']] = so.relationship(back_populates='materialised_measure', cascade='all,delete,delete-orphan')

class Measure_Person_Map(Base):
    __tablename__ = 'measure_person_map' 
    measure_person_map_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    materialised_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('materialised_measure.measure_id'))
    id = so.synonym('materialised_measure_id')

    person_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('person.person_id'))
    measure_date: so.Mapped[date] = so.mapped_column(sa.DateTime)

    materialised_measure: so.Mapped['Materialised_Measure'] = so.relationship(foreign_keys=[materialised_measure_id], back_populates='measure_people')
    person: so.Mapped['Person'] = so.relationship("Person", foreign_keys=[person_id])

class Materialised_Cohort(Base):
    __tablename__ = 'materialised_cohort'

    dash_cohort_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'), primary_key=True)
    id = so.synonym('dash_cohort_id')
    refresh_date: so.Mapped[date] = so.mapped_column(sa.DateTime, server_default=sa.sql.func.now())

    dash_cohort: so.Mapped['Dash_Cohort'] = so.relationship('Dash_Cohort', foreign_keys=[dash_cohort_id])
    materialised_cohort_defs: so.Mapped[List['Materialised_Cohort_Def']] = so.relationship(back_populates='materialised_cohort', cascade='all,delete,delete-orphan')
    definitions = so.synonym('materialised_cohort_defs')

    @sa.ext.hybrid.hybrid_property
    def dash_cohort_name(self):
        if self.dash_cohort:
            return self.dash_cohort.dash_cohort_name

class Materialised_Cohort_Def(Base):
    __tablename__ = 'materialised_cohort_def'
    dash_cohort_def_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('dash_cohort_def.dash_cohort_def_id'), primary_key=True)
    id = so.synonym('dash_cohort_def_id')

    materialised_cohort_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('materialised_cohort.dash_cohort_id'))
    refresh_date: so.Mapped[date] = so.mapped_column(sa.DateTime, server_default=sa.sql.func.now())

    dash_cohort_def: so.Mapped['Dash_Cohort_Def'] = so.relationship('Dash_Cohort_Def', foreign_keys=[dash_cohort_def_id])
    materialised_cohort: so.Mapped['Materialised_Cohort'] = so.relationship('Materialised_Cohort', foreign_keys=[materialised_cohort_id], back_populates='materialised_cohort_defs')
    members: so.Mapped[List['Cohort_Person_Map']] = so.relationship(back_populates='materialised_cohort_def', cascade='all,delete,delete-orphan')

    @sa.ext.hybrid.hybrid_property
    def dash_cohort_def_name(self):
        if self.dash_cohort_def:
            return self.dash_cohort_def.dash_cohort_def_name
    
class Cohort_Person_Map(Base):
    __tablename__ = "cohort_person_map"
    
    materialised_cohort_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('materialised_cohort_def.dash_cohort_def_id'), primary_key=True)
    id = so.synonym('materialised_cohort_def_id')

    person_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('person.person_id'), primary_key=True)
    measure_date: so.Mapped[date] = so.mapped_column(sa.DateTime, primary_key=True)

    materialised_cohort_def: so.Mapped['Materialised_Cohort_Def'] = so.relationship(foreign_keys=[materialised_cohort_def_id], back_populates='members')
    person: so.Mapped['Person'] = so.relationship("Person", foreign_keys=[person_id])

# class Materialised_Report(Base):
#     __tablename__ = 'materialised_report'
#     materialised_report_id: so.Mapped[int] = so.mapped_column(primary_key=True) 
#     report_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('report.report_id'))
#     id = so.synonym('report_id')
    
#     report_version_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('report_version.report_version_id'))
    
#     refresh_date: so.Mapped[date] = so.mapped_column(sa.DateTime, server_default=sa.sql.func.now())
#     report_version: so.Mapped["Report_Version"] = so.relationship(foreign_keys=[report_version_id])

#     report_people: so.Mapped[List['Report_Person_Map']] = so.relationship(back_populates='materialised_report')


# class Report_Person_Map(Base):
#     __tablename__ = 'report_person_map'
    
#     materialised_report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('materialised_report.materialised_report_id'), primary_key=True)
#     id = so.synonym('materialised_report_id')

#     person_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('person.person_id'), primary_key=True)
#     measure_id: so.Mapped[int] =  so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True)

#     measure_date: so.Mapped[date] = so.mapped_column(sa.DateTime)

#     materialised_report: so.Mapped['Materialised_Report'] = so.relationship(foreign_keys=[materialised_report_id], back_populates='report_people')
#     person: so.Mapped["Person"] = so.relationship(foreign_keys=[person_id])
#     measure: so.Mapped["Measure"] = so.relationship(foreign_keys=[measure_id])
