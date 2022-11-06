from query.querygraph import *
from solve.generate import *
from solve.build import QueryBuilder
from rank.freebase_tag import FreebaseTagger
from rank.wikidata_tag import WikidataTagger
from utils.wikidata_utils import *
from evaluate.evaluate import Evaluator

class TemporalQuestionSolver():
    def __init__(self,ground_kb,path_rank_model_path,graph_rank_model_path):
        self.ground_kb = ground_kb
        self.ranker = BertRanker(graph_rank_model_path)
        self.generator = TemporalQueryGenerator(ground_kb,path_rank_model_path)
        if ground_kb == "FREEBASE":
            self.tagger = FreebaseTagger()
        else:
            self.tagger = WikidataTagger()

    def solve(self,parsed_question,k=1,t=None):
        graphs = self.generator.generate(parsed_question)
        if len(graphs) == 0:
            return set()

        labels = [self.tagger.tag(graph) for graph in graphs]
        scores = self.ranker.rank(parsed_question.question,labels)
        for graph,score in zip(graphs,scores):
            graph.score = score
        graphs.sort(key=lambda g:g.score,reverse=True)
        answers = set()
        cnt = 0
        for graph in graphs:
            cur_answers = graph.get_answers()
            if len(cur_answers) != 0:
                print("query_graph:",graph.to_label_seq())
                print("sparql:",graph.to_sparql())
                cnt += 1
                answers = answers | set(cur_answers)
            if cnt == k:
                break
        if cnt == 0:
            print("query_graph:")
            print("sparql:")
        return answers

    def get_all_answer_list(self,parsed_question,k=1,t=None):
        graphs = self.generator.generate(parsed_question)
        answer_list = []
        cnt = 0
        for graph in graphs:
            cur_answers = graph.get_answers()
            if len(cur_answers) != 0:
                cnt += 1
                answer_list.append(set(cur_answers))
        if cnt == 0:
            print("query_graph:")
            print("sparql:")
        return answer_list

    def get_max_f1_res(self,parsed_question):
        graphs = self.generator.generate(parsed_question)
        answer_res = set()
        query_graph = None
        sparql = None
        cnt = 0

        labels = [self.tagger.tag(graph) for graph in graphs]
        scores = self.ranker.rank(parsed_question.question, labels)
        for graph, score in zip(graphs, scores):
            graph.score = score
        graphs.sort(key=lambda g: g.score, reverse=True)

        evaluator = Evaluator([], parsed_question.answers)
        for graph in graphs:
            cur_answers = graph.get_answers()
            if len(cur_answers) != 0:
                cnt += 1
                cur_evaluator = Evaluator(cur_answers, parsed_question.answers)
                if (cur_evaluator.f1 > evaluator.f1):
                    evaluator = cur_evaluator
                    answer_res = cur_answers
                    query_graph = graph.to_label_seq()
                    sparql = graph.to_sparql()
        if cnt == 0:
            print("query_graph:")
            print("sparql:")
        else:
            if(len(answer_res)==0):
                answer_res = graphs[0].get_answers()
                query_graph = graphs[0].to_label_seq()
                sparql = graphs[0].to_sparql()
            print("query_graph:", query_graph)
            print("sparql:", sparql)
        return answer_res
    
    def predict_top_queries(self,parsed_question,k=1,t=None):
        graphs = self.generator.generate(parsed_question)
        if len(graphs) == 0:
            return []

        labels = [self.tagger.tag(graph) for graph in graphs]
        scores = self.ranker.rank(parsed_question.question, labels)
        for graph, score in zip(graphs, scores):
            graph.score = score
        graphs.sort(key=lambda g: g.score, reverse=True)
        
        if t is None:
            top_graphs = graphs[:k]
        else:
            top_graphs = [graph.score > t for graph in graphs]
        return top_graphs

                        
