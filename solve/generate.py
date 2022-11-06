from rank.ranker import BertRanker
from query.querygraph import TIME_VALUE_RELATION
from solve.build import QueryBuilder

class TemporalQueryGenerator():
    def __init__(self,ground_kb,path_rank_model_path,tlink2algebra_path="data/tlink2algebra.txt"):
        self.tlink2algebra = {}
        self.ground_kb = ground_kb
        with open(tlink2algebra_path,"rt",encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                tlink_relations = line.split("#")[0].split(",")
                query_relations = line.split("#")[1].split(",")
                query_relations = [TIME_VALUE_RELATION[relation] for relation in query_relations]
                for tlink_relation in tlink_relations:
                    self.tlink2algebra[tlink_relation] = query_relations
        
        if ground_kb == "WIKIDATA":
            import query.wikidata_element as we
            import query.wikidata_template as wt
            we.ranker = BertRanker(path_rank_model_path)
            wt.ranker = BertRanker(path_rank_model_path)
        elif ground_kb == "FREEBASE":
            import query.freebase_element as fe
            import query.freebase_template as ft
            fe.ranker = BertRanker(path_rank_model_path)
            ft.ranker = BertRanker(path_rank_model_path)

    def generate(self,parsed_question):
        builder = QueryBuilder(self.ground_kb,parsed_question,self.tlink2algebra)
        return builder.generate()