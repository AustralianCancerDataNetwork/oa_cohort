# from .cohorts import Dash_Cohort, Dash_Cohort_Def, Dash_Cohort_Rule, Dash_Cohort_Dx, Dash_Cohort_Dx_Rule, Dash_Cohort_Tx_Rule, RuleCombination, RuleType, DxType, TxType

# __all__ = [Dash_Cohort, Dash_Cohort_Def, Dash_Cohort_Rule, Dash_Cohort_Dx, Dash_Cohort_Dx_Rule, Dash_Cohort_Tx_Rule,  RuleCombination, RuleType, DxType, TxType]

from .reports import Report, Report_Version, Dash_Cohort, Dash_Cohort_Def, Measure, Measure_Relationship, Subquery, Query_Rule, Indicator, Base, Report_Cohort_Map, dash_cohort_measure_map
#from .cohorts import Subquery_Person, Measure_Person, Cohort_Person, Cohort_Refresh
from .materialised_report import Materialised_Measure, Measure_Person_Map#, Cohort_Person_Map, Report_Person_Map, Materialised_Cohort_Def,  Materialised_Report, Materialised_Cohort

__all__ = [Report, Report_Version, Dash_Cohort, Dash_Cohort_Def, Measure, Subquery, 
           Query_Rule, Base, Report_Cohort_Map, dash_cohort_measure_map, Materialised_Measure, Measure_Person_Map] # Cohort_Person_Map, Report_Person_Map, Materialised_Cohort_Def,
           #Subquery_Person, Measure_Person, Cohort_Person, Cohort_Refresh, Materialised_Report, Materialised_Cohort, ]




# import sqlalchemy as sa
# import sqlalchemy.orm as so
# from omop_alchemy import Condition_Occurrence
# from oa_cohorts import Query_Rule, Subquery, Measure, Measure_Def
# from oa_configurator import oa_config

# db = so.Session(oa_config.engine)

# s1 = db.get(Query_Rule, 144)
# s2 = db.get(Query_Rule, 145)

# cc1 = s1.get_query(db)
# cc2 = s2.get_query(db)

# q1 = db.query(Condition_Occurrence).filter(s1.get_filter_details(Condition_Occurrence.condition_code))
# q2 = db.query(Condition_Occurrence).filter(s2.get_filter_details(Condition_Occurrence.condition_code))

# q = q1.union(q2)

# print(q.statement.compile(compile_kwargs={'literal_binds':True}))

# m = db.get(Measure_Def, 84)

# def get_measure_def(self, db):
#     if self.subqueries:
#         return self.subqueries.get_subquery(db)
#     elif self.child_measures:
#         cm = [md.execute_measure_def(db) for md in self.child_measures.measure_defs(db)]
#         query = cm[0]
#         for q in cm[1:]:
#             if self.subquery_combination in [RuleCombination.rule_or, RuleCombination.rule_simple]:
#                 query = query.union(q)
#             elif self.subquery_combination in [RuleCombination.rule_and]:
#                 query = query.intersection(q)
#         return query

# def execute_measure_def(self, db):
#     return self.get_measure_def(db).all()
# #     def combiner(self):
# #         return self.measure.measure_combination.combiner()

# #     def get_filter(self):
# #         if self.child_measures:
# #             return self.child_filters() 
# #         if self.subqueries:
# #             return self.my_filter()  

# #     def my_filter(self):
# #         return self.combiner()(*[self.subqueries.get_filter()])

# #     def child_filters(self):
# #         return self.combiner()(*[sq.get_filter() for sq in self.child_measures.measure_defs])

# #     def execute_subquery(self, db):
# #         if len(self.query_rules) == 0:
# #             return None
# #         qr = [db.query(self.filter_table()).filter(sq.get_filter_details(self.filter_field)) for sq in self.query_rules]
# #         query = qr[0]
# #         for q in qr[1:]:
# #             if self.subquery_combination in [RuleCombination.rule_or, RuleCombination.rule_simple]:
# #                 query = query.union(q)
# #             elif self.subquery_combination in [RuleCombination.rule_and]:
# #                 query = query.intersection(q)
# #         return query.all()




# # def subq_to_cohort(db, subq_id):
# #     subq = db.get(models.Subquery, subq_id)
# #     if not subq: 
# #         return JSONResponse(status_code=404, content={"message": f"Subquery with ID {subq_id} not found"})
# #     results = subq.execute_subquery(db)
# #     return list(set([r.person_id for r in results]))
    