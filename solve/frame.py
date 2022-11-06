from query.wikidata_template import *
from query.freebase_template import *
'''
        Default RF, use when no reasoning is detected as needed
            Ans -- E1. Ans -- E2. ... Ans -- En
    '''


class BasicReasoningFrame:  # BasicT
    '''
        single aspect of E1
            Ans -- E1
    '''

    def __init__(self):
        super()

    def is_useable(temporal_relation) -> bool:
        temp_rel = ["BEFORE","AFTER","IBEFORE","IAFTER"]
        return not temporal_relation.rel_type in temp_rel

    def make_with(self,ground_kb):
        if ground_kb == "WIKIDATA":
            return BRFonWikidata()
        else:
            return BRFonFreebase()

class AlgebraicFrame(BasicReasoningFrame):  # ValueT
    '''
        Use when two time values satisfies certain algebraic predicate
            Ans in target.aspects, pred(time(target), relatedTo)
            or
            Ans in targeet.aspects, pred(time(target), time(relatedTo))
    '''

    def make_with(temporal_relation, ground_kb, candidate_relations):
        if ground_kb == "WIKIDATA":
            return AFonWikidata(temporal_relation, candidate_relations)
        else:
            return AFonFreebase(temporal_relation, candidate_relations)


class TemporalAspectFrame(BasicReasoningFrame):  # PropertyT
    '''
        has same time T
            S = Ans -- E, S -- T
    '''
    def is_useable(temporal_relation) -> bool:
        same_time_rel = ["INCLUDE","SIMULTANEOUS","BEGIN","END"]
        return temporal_relation.related.type == "T1" and temporal_relation.rel_type in same_time_rel

    def make_with(temporal_relation, ground_kb):
        if ground_kb == "WIKIDATA":
            return TAsFonWikidata(temporal_relation)
        else:
            return TAsFonFreebase(temporal_relation)


class TemporalAnsFrame(BasicReasoningFrame):  # AnswerT
    '''
        Use when the query target is an temoral aspect of certain object.
            Ans -- E, Ans is Time
    '''
    def is_useable(queryBuilder) -> bool:
        return queryBuilder.target_type == "temporal"

    def make_with(temporal_ans_cons, ground_kb):
        if ground_kb == "WIKIDATA":
            return TAFonWikidata(temporal_ans_cons)
        else:
            return TAFonFreebase(temporal_ans_cons)

class PartOfFrame(BasicReasoningFrame):  # IncludeT*
    '''
        Use when one eventuality is a part of the other one
            S = (Ans -- E1), S partOf E2
            or
            Ans -- E1, E1 partOf E2
    '''
    def is_useable(temporal_relation) -> bool:
        return temporal_relation.rel_type == "INCLUDE" and not temporal_relation.related.type == "T1"

    def make_with(temporal_relation, ground_kb):
        if ground_kb == "WIKIDATA":
            return POFonWikidata(temporal_relation)
        else:
            return POFonFreebase(temporal_relation)


class SingularFrame(BasicReasoningFrame):  # IncludeT*
    '''
        Use when the eventualities are different aspects of one object
            Ans -- X, X -- E1, X -- E2
    '''
    def is_useable(temporal_relation) -> bool:
        same_time_rel = ["INCLUDE", "SIMULTANEOUS", "BEGIN", "END"]
        return temporal_relation.rel_type in same_time_rel

    def make_with(temporal_relation, ground_kb):
        if ground_kb == "WIKIDATA":
            return SiFonWikidata(temporal_relation)
        else:
            return SiFonFreebase(temporal_relation)

class TemporalAspectForOrdinalFrame(BasicReasoningFrame):
    def is_useable(queryBuilder) -> bool:
        return queryBuilder.ordinal_relations

    def make_with(temporal_ordinal, ground_kb):
        template_list = []
        if ground_kb == "WIKIDATA":
            if (temporal_ordinal.rank == -1 or temporal_ordinal.rank == 1):
                template_list.append(TAsFOonWikidata(temporal_ordinal, constraint_type="predicate"))
            template_list.append(TAsFOonWikidata(temporal_ordinal, constraint_type="num"))
            return template_list
        else:
            if (temporal_ordinal.rank == -1 or temporal_ordinal.rank == 1):
                template_list.append(TAsFOonFreebase(temporal_ordinal, constraint_type="predicate"))
            template_list.append(TAsFOonFreebase(temporal_ordinal, constraint_type="num"))
            return template_list


class OrdinalFrame(BasicReasoningFrame):  # OrdinalT
    '''
        Use when there is an explicit ordinal value
            Ans -- E, E -- o #o could be a part of certain rdfs:label
            or
            Ans -- E0, rank(E0, Eset) = o
    '''
    def is_useable(queryBuilder) -> bool:
        return queryBuilder.ordinal_relations

    def make_with(temporal_ordinal, ground_kb):
        if ground_kb == "WIKIDATA":
            return OFonWikidata(temporal_ordinal)
        else:
            return OFonFreebase(temporal_ordinal)


class SequentialFrame(BasicReasoningFrame):  # CompareT
    '''
        Use when the eventualities make a sequent
            Ans -- E1, E1 concequent E2
    '''

    def is_useable_for_temporal_relation(temporal_relation):
        if (temporal_relation.rel_type == "IBEFORE" or temporal_relation.rel_type=="IAFTER"):
            return True
        if (not temporal_relation.related.type == "T1" and (temporal_relation.signal)):
            if (temporal_relation.signal.text == "before" or temporal_relation.signal.text == "after"):
                return True
        return False

    def is_useable(question) -> bool:
        trigers = ["follows", "preceded by", "followed by", "precedes", "preceded", "succeeded"]
        for trigger in trigers:
            if(trigger in question):
                return True
        return False

    def make_with(ground_kb):
        if ground_kb == "WIKIDATA":
            return STFonWikidata()
        else:
            return STFonFreebase()


class SuccessivelFrame(BasicReasoningFrame):  # CompareT
    '''
        Use when the eventualities make a sequent
            Ans -- E1, E1 concequent E2
    '''

    def is_useable(temporal_relation) -> bool:
        if (temporal_relation.rel_type == "IEFORE" or temporal_relation.rel_type=="IAFTER"):
            return True
        if (not temporal_relation.related.type == "T1" and (temporal_relation.signal)):
            if (temporal_relation.signal.text == "before" or temporal_relation.signal.text == "after"):
                return True
        return False

    def make_with(temporal_relation, ground_kb):
        if ground_kb == "WIKIDATA":
            return SFonWikidata(temporal_relation)
        else:
            return SFonFreebase(temporal_relation)




