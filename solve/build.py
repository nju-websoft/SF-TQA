from query.querygraph import *
from solve.frame import *

def select_graphs_with_answers(graphs):
    new_graph_list = []
    for graph in graphs:
        results = graph.execute()
        if(len(results)>0):
            new_graph_list.append(graph)
    return new_graph_list

class QueryBuilder():
    def __init__(self, ground_kb, parsed_question, tlink2algebra):
        self.tlink2algebra = tlink2algebra
        self.ordinal_relations = False
        self.time_relations = []
        self.ground_kb = ground_kb
        if ground_kb == "WIKIDATA":
            self.tagger = WikidataPathRankTagger()
        elif ground_kb == "FREEBASE":
            self.tagger = FreebasePathRankTagger()
        self.question = parsed_question
        if (parsed_question.rec_temporal_ans_cons):
            self.target_type = "temporal"
        else:
            self.target_type = "entity"
        if (len(parsed_question.rec_temporal_ordinals) > 0):
            self.ordinal_relations = True
        if (len(parsed_question.rec_temporal_relations) > 0):
            temporal_relations = parsed_question.rec_temporal_relations
            event_list = []
            for temporal_relation in temporal_relations:
                event_list.append(temporal_relation.target.id)
                event_list.append(temporal_relation.related.id)
            self.time_relations.append(event_list)

    def generate(self):
        entities = [link.uri for link in self.question.entity_linking.values()]
        if (TemporalAnsFrame.is_useable(self)):
            template = TemporalAnsFrame.make_with(self.question.rec_temporal_ans_cons, self.ground_kb)
            graphs = template.template_grounding(self.question)
        else:
            graphs = []
            if(OrdinalFrame.is_useable(self)):
                graph = []
                template_list = []
                template_list.append(OrdinalFrame.make_with(self.question.rec_temporal_ordinals[0], self.ground_kb))
                template_list += TemporalAspectForOrdinalFrame.make_with(self.question.rec_temporal_ordinals[0], self.ground_kb)
                for template in template_list:
                    graph += template.template_grounding(self.question)
                if(len(graph)):
                    graphs += graph
            if(len(self.question.rec_temporal_relations)):
                t_graphs = []
                for temporal_relation in self.question.rec_temporal_relations:
                    frame_list = []
                    alFrame = AlgebraicFrame.make_with(temporal_relation, self.ground_kb, self.tlink2algebra[temporal_relation.rel_type])
                    graph = alFrame.template_grounding(self.question)
                    if (SuccessivelFrame.is_useable(temporal_relation)):
                        sufFrame = SuccessivelFrame.make_with(temporal_relation, self.ground_kb)
                        graph = sufFrame.template_grounding(graph)
                    if(SequentialFrame.is_useable_for_temporal_relation(temporal_relation)):
                        frame_list.append(SequentialFrame.make_with(self.ground_kb))
                    if(PartOfFrame.is_useable(temporal_relation)):
                        frame_list.append(PartOfFrame.make_with(temporal_relation, self.ground_kb))
                    if(SingularFrame.is_useable(temporal_relation)):
                        frame_list.append(SingularFrame.make_with(temporal_relation, self.ground_kb))
                    if (TemporalAspectFrame.is_useable(temporal_relation)):
                        frame_list.append(TemporalAspectFrame.make_with(temporal_relation, self.ground_kb))
                    for template in frame_list:
                        graph += template.template_grounding(self.question)
                    if(len(graph)):
                        t_graphs.append(graph)
                if(len(t_graphs) > 1):
                    # graphs += t_graphs[0] + t_graphs[1]
                    graphs += mergeGraph(t_graphs)
                elif(len(t_graphs)==1):
                    graphs += t_graphs[0]
            if(len(graphs)==0 and SequentialFrame.is_useable(self.question.question)):
                template = SequentialFrame.make_with(self.ground_kb)
                graph = template.template_grounding(self.question)
                if(len(graph)):
                    graphs += graph
        graphs = self.add_entity_cons(graphs,self.question)
        graphs = self.add_numerical_cmp_cons(graphs,self.question)
        graphs += BasicReasoningFrame().make_with(ground_kb=self.ground_kb).template_grounding(self.question)
        graphs = self.filter_graphs(graphs)
        return graphs

    def add_entity_cons(self, graphs, parsed_question):
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        for entity in entities:
            new_graphs = []
            for graph in graphs:
                new_graphs += self.connect_graph_with_entity(graph, entity)
            graphs += new_graphs
        return graphs

    def add_numerical_cmp_cons(self, graphs, parsed_question):
        numerical_cmp_cons_list = parsed_question.rec_numerical_cmp_cons_list
        if len(numerical_cmp_cons_list) == 0:
            return graphs
        new_graphs = list(graphs)
        for numerical_cmp_cons in numerical_cmp_cons_list:
            for graph in graphs:
                for var in graph.vars:
                    num_var = "var" + str(graph.node_id)
                    tmp_graph = graph.copy()
                    tmp_graph.add_triple(var, "p", num_var)
                    tmp_graph.add_numerical_cmp(num_var, numerical_cmp_cons.cmp_sign, numerical_cmp_cons.number)
                    tmp_graph.set_answer_vars(["p"])
                    results = tmp_graph.execute()
                    for result in results:
                        prop = result["p"]
                        new_graph = graph.copy()
                        new_graph.add_triple(var, prop, num_var)
                        new_graph.add_numerical_cmp(num_var, numerical_cmp_cons.cmp_sign, numerical_cmp_cons.number)
                        new_graphs.append(new_graph)
        return new_graphs

    def connect_graph_with_entity(self, graph, entity, limit=-1):
        new_graphs = []
        if entity in graph.entities:
            return []
        for var in graph.vars:
            tmp_graph = graph.copy()
            tmp_graph.add_triple(var, "p", entity)
            tmp_graph.set_answer_vars(["p"])
            results = tmp_graph.execute()
            for result in results:
                prop = result["p"]
                new_graph = graph.copy()
                new_graph.add_triple(var, prop, entity)
                new_graphs.append(new_graph)

            tmp_graph = graph.copy()
            tmp_graph.add_triple(entity, "p", var)
            tmp_graph.set_answer_vars(["p"])
            results = tmp_graph.execute()
            for result in results:
                prop = result["p"]
                new_graph = graph.copy()
                new_graph.add_triple(entity, prop, var)
                new_graphs.append(new_graph)
        return new_graphs

    def filter_graphs(self, graphs):
        query_label_set = set()
        new_graphs = []
        for graph in graphs:
            label = self.tagger.tag(graph)
            if(label in query_label_set):
                continue
            query_label_set.add(label)
            new_graphs.append(graph)
        return new_graphs


def mergeGraph(graphs,canidate_num=10):
    graphs1,graphs2 = graphs[0],graphs[1]
    if(len(graphs1)>canidate_num):
        graphs1 = graphs1[:canidate_num]
    if (len(graphs2) > 5):
        graphs2 = graphs2[:canidate_num]
    new_graphs = []
    for graph1 in graphs1:
        for graph2 in graphs2:
            new_graphs.append(merge_graph(graph1,graph2))
    new_graphs = select_graphs_with_answers(new_graphs)
    return new_graphs+graphs[0]

