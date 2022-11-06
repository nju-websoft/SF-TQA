from utils.wikidata_utils import *
from query.querygraph import QueryGraph, TIME_VALUE_RELATION, Edge
from query.wikidata_element import *
from rank.ranker import BertRanker
from rank.wikidata_tag import WikidataPathRankTagger
import re

ranker = None
seq_ranker = BertRanker("data/models/path_rank_tq_best_neg5_seq_3090_new.pth")
part_ranker = BertRanker("data/models/path_rank_tq_best_neg5_part_3090_new.pth")
tagger = WikidataPathRankTagger()
canidate_num = 5

def filter_graphs(graphs,limit=None):
    new_graph_list = []
    for graph in graphs:
        results = graph.execute()
        if(len(results)>0):
            new_graph_list.append(graph)
        if(limit):
            if(len(new_graph_list)==limit):
                return new_graph_list
    return new_graph_list


class BRFonWikidata():#mul aspect for one entity
    def __init__(self):
        self.frame_name = "BasicReasoningFrame"

    def template_grounding(self,parsed_question):
        graphs = self.generate_onehop_bone(parsed_question, limit=canidate_num)
        return graphs

    def generate_onehop_bone(self,parsed_question, limit=canidate_num):
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        graphs = []
        for entity in entities:
            out_paths = query_onehop_out_paths(entity)
            for out_path in out_paths:
                graph = QueryGraph(parsed_question)
                graph.add_triple(entity, out_path[0], "s0")
                graph.add_triple("s0", out_path[1], "ans")
                graph.add_answer_var("ans")
                graph.set_frame_name(self.frame_name)
                graphs.append(graph)

            in_paths = query_onehop_in_paths(entity)
            for in_path in in_paths:
                graph = QueryGraph(parsed_question)
                graph.add_triple("ans", in_path[1], "s0")
                graph.add_triple("s0", in_path[0], entity)
                graph.add_answer_var("ans")
                graph.set_frame_name(self.frame_name)
                graphs.append(graph)
        if limit and len(graphs) > limit:
            labels = [tagger.tag(graph) for graph in graphs]
            scores = ranker.rank(parsed_question.question, labels)
            for graph, score in zip(graphs, scores):
                graph.score = score
            graphs.sort(key=lambda g: g.score, reverse=True)
            graphs = graphs[:limit]
        return graphs

class AFonWikidata(BRFonWikidata):#alg
    def __init__(self,temporal_relation,candidate_relations):
        self.frame_name = "AlgebraicFrame"
        self.candidate_relations = candidate_relations
        self.type = temporal_relation.rel_type
        self.constraint_type = "value"
        self.constraints = []
        if (temporal_relation.target.type == "E3"):
            self.target = Property(temporal_relation.target, "property", var="target")
        else:
            self.target = Event(temporal_relation.target, var="target")
        if(temporal_relation.related.type == "T1"):
            temporal_relation.related.interval.append(temporal_relation.related.text)
            self.related = Event_for_nominal(temporal_relation.related,interval=temporal_relation.related.interval)
        else:
            self.related = Event(temporal_relation.related, "related")

    def template_grounding(self, parsed_question):
        candidate_graphs = []
        point_relations = [TIME_VALUE_RELATION.BEFORE, TIME_VALUE_RELATION.AFTER, TIME_VALUE_RELATION.MET_BY,
                           TIME_VALUE_RELATION.BEGIN, TIME_VALUE_RELATION.OVERLAP]
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        graph = QueryGraph(parsed_question)
        graph_list = []
        for entity in entities:
            rawgraph = graph.copy()
            target_graph = self.target.grounding(entity=entity, rawgraph=rawgraph, question=parsed_question.question, limit=canidate_num)
            for graph in target_graph:
                graph_list += extend_graph_with_temporal_predicate(graph)
        if(self.related.interval):
            for graph, time_var in graph_list:
                in_edge = graph.get_node_in_edges(name=time_var)[0]
                is_point = False
                if type(in_edge.value) == type(""):
                    is_point = True
                for temporal_relation in self.candidate_relations:
                    if is_point and temporal_relation not in point_relations:
                        continue
                    tmp_graph = graph.copy()
                    tmp_graph.add_time_value_relation(time_var, temporal_relation, self.related.interval)
                    tmp_graph.set_answer_vars(["targetAspect"])
                    tmp_graph.set_frame_name(self.frame_name)
                    results = tmp_graph.execute()
                    if(len(results)):
                        candidate_graphs.append(tmp_graph)
        else:
            related_graphs = self.related.grounding_for_related_event_with_time(parsed_question, graph)
            target_graphs = graph_list
            for target_graph, target_var in target_graphs:
                for related_graph, related_var in related_graphs:
                    if(target_graph.is_contains(related_graph)):
                        continue
                    for value_relation in self.candidate_relations:
                        tmp_graph = target_graph.copy()
                        tmp_graph.set_frame_name(self.frame_name)
                        # combine graph
                        for edge in related_graph.edges:
                            from_val = related_graph.find_node(edge.from_id).value
                            to_val = related_graph.find_node(edge.to_id).value
                            if edge.type == Edge.URI:
                                tmp_graph.add_triple(from_val, edge.value, to_val)
                            else:
                                tmp_graph.add_time_interval(from_val, edge.value, to_val)
                        # relation filter implemention
                        tmp_graph.add_time_value_relation(target_var, value_relation, related_var)
                        tmp_graph.set_answer_vars(["targetAspect"])
                        results = tmp_graph.execute()
                        if (len(results)):
                            candidate_graphs.append(tmp_graph)
        return candidate_graphs

class POFonWikidata(BRFonWikidata):
    def __init__(self,temporal_relation):
        self.frame_name = "PartOfFrame"
        self.target = Event(temporal_relation.target)

    def template_grounding(self,parsed_question):
        '''
                Use when one eventuality is a part of the other one
                    S = (Ans -- E1), S partOf E2
                    or
                    Ans -- E1, E1 partOf E2
        '''
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        rawgraph = QueryGraph(parsed_question)
        rawgraph.set_frame_name(self.frame_name)
        graph_list = []
        target_graphs = []
        if (len(entities) == 2):
            ##statement
            graphs = self.target.grounding_for_two_entities(entities[0], entities[1], rawgraph)
            if(len(graphs)==0):
                entities[0],entities[1] = entities[1],entities[0]
                graphs = self.target.grounding_for_two_entities(entities[0], entities[1], rawgraph)
            for graph in graphs:
                labels = [query_label(parse_id(edge.value)) for edge in graph.edges]
                scores = part_ranker.rank(parsed_question.question, labels)
                if(max(scores)>0):
                    target_graphs.append(graph)
                # is_part = False
                # for edge in graph.edges:
                #     if(parse_id(edge.value) in part_of_predicates):
                #         is_part = True
                #         break
                # if(is_part):
                #     target_graphs.append(graph)
            for graph in target_graphs:
                new_graph = graph.copy()
                new_graph.add_triple("s0", "p", "ans")
                new_graph.set_answer_vars(["p"])
                results = new_graph.execute()
                for result in results:
                    prop = result["p"]
                    candidate_graph = graph.copy()
                    candidate_graph.add_triple("s0", prop, "ans")
                    candidate_graph.set_answer_vars(["ans"])
                    candidate_graph.add_answer_type()
                    graph_list.append(candidate_graph)
            ##entities
            if(len(graph_list)==0 and len(target_graphs)>0):
                event_entity = [e for e in entities if e in time_event]
                if(len(event_entity)):
                    for entity in event_entity:
                        for graph in target_graphs:
                            new_graph = graph.copy()
                            new_graph.add_triple(entity, "p", "ans")
                            new_graph.set_answer_vars(["p"])
                            results = new_graph.execute()
                            for result in results:
                                prop = result["p"]
                                candidate_graph = graph.copy()
                                candidate_graph.add_triple("s0", prop, "ans")
                                candidate_graph.set_answer_vars(["ans"])
                                candidate_graph.add_answer_type()
                                graph_list.append(candidate_graph)
        if(len(graph_list)>canidate_num):
            labels = [tagger.tag(graph) for graph in graph_list]
            scores = ranker.rank(parsed_question.question, labels)
            for graph, score in zip(graph_list, scores):
                graph.score = score
            graph_list.sort(key=lambda g: g.score, reverse=True)
            graph_list = graph_list[:canidate_num]
        return graph_list


class SiFonWikidata(BRFonWikidata):# mul aspect for two entity
    '''
            Use when the eventualities are different aspects of one object
                Ans -- X, X -- E1, X -- E2
    '''
    def __init__(self, temporal_relation):
        self.frame_name = "SingularFrame"
        self.constraint_type = "entity"
        self.target = Event(temporal_relation.target)
        self.time = None
        if (temporal_relation.related.type == "T1"):
            time_regex = re.compile("\d{4}")
            time_text = time_regex.match(temporal_relation.related.text)
            self.related_text = time_text.group(0) if time_text else None
            self.time = temporal_relation.related.interval

    def template_grounding(self,parsed_question):
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        graph = QueryGraph(parsed_question)
        graph.set_frame_name(self.frame_name)
        graph_list = []
        target_graphs = []
        if(self.time):##T1
            graphs = []
            for entity in entities:
                graphs += self.target.grounding(entity, graph, parsed_question.question)
            for graph in graphs:
                target_graphs = extend_graph_with_temporal_predicate(graph, var="s0")
                for new_graph, time_var in target_graphs:
                    new_graph.add_time_value_relation(time_var, TIME_VALUE_RELATION.EQUAL, self.time)
                    new_graph.set_answer_vars(["s0Aspect"])
                    graph_list.append(new_graph)
            graph_list = filter_graphs(graph_list)
            index = self.time[0].index("^^")
            time_mention = self.time[0][:index].replace("\"","")
            is_t1_included_by_e = False##时间被名字或者周围实体表达
            for entity in entities:
                if(self.related_text and self.related_text in query_label(entity)):
                    is_t1_included_by_e = True
                    break
                time_res = get_time_direct_entity(entity)
                if(time_mention in time_res):
                    is_t1_included_by_e = True
                    break
            if(is_t1_included_by_e):
                graph_list += self.generate_onehop_bone(parsed_question, limit=canidate_num)
                for graph in graph_list:
                    graph.set_frame_name(self.frame_name)
            return graph_list
        if (len(entities) == 2):
            target_graphs += self.target.grounding_for_two_entities(entities[0], entities[1], graph)
        elif(len(entities)==1):
            target_graphs += self.target.grounding_to_time_event(entities[0], graph)
        for graph in target_graphs:
            new_graph = graph.copy()
            new_graph.add_triple("s0", "p", "ans")
            new_graph.set_answer_vars(["p"])
            results = new_graph.execute()
            for result in results:
                prop = result["p"]
                candidate_graph = graph.copy()
                candidate_graph.add_triple("s0", prop, "ans")
                candidate_graph.set_answer_vars(["ans"])
                candidate_graph.add_answer_type()
                graph_list.append(candidate_graph)
        if (len(graph_list) >canidate_num):
            labels = [tagger.tag(graph) for graph in graph_list]
            scores = ranker.rank(parsed_question.question, labels)
            for graph, score in zip(graph_list, scores):
                graph.score = score
            graph_list.sort(key=lambda g: g.score, reverse=True)
            graph_list = graph_list[:canidate_num]
        return graph_list

class TAsFOonWikidata(BRFonWikidata):
    def __init__(self, temporal_ordinals, constraint_type=None):  ##constraint_type="num" or "value" or "predicate"
        self.frame_name = "TemporalAspectForOrdinalFrame"
        self.constraint_type = constraint_type
        self.rank = temporal_ordinals.rank
        self.target = None
        if (temporal_ordinals.target):
            self.target = Event(temporal_ordinals.target)

    def template_grounding(self,parsed_question):
        graphs = []
        entity_graphs = self.generate_onehop_bone(parsed_question)
        if(self.constraint_type=="predicate"):
            for graph in entity_graphs:
                graph.set_frame_name(self.frame_name)
                labels = ""
                for edge in graph.edges:
                    labels += query_label(edge.value)+" "
                if(self.rank==1 and "first" in labels):
                        graphs.append(graph)
                if(self.rank==-1 and "last" in labels):
                    graphs.append(graph)
        elif(self.rank>0):
            for graph in entity_graphs:
                var_name = "var" + str(graph.node_id)
                graph.set_frame_name(self.frame_name)
                tmp_graph = graph.copy()
                tmp_graph.add_triple("s0", get_uri("pq:P1545"), var_name)
                tmp_graph.add_numerical_cmp(var_name, "=", self.rank)
                results = tmp_graph.execute()
                if(len(results)):
                    graphs.append(tmp_graph)
        return graphs

class OFonWikidata(BRFonWikidata):
    def __init__(self,temporal_ordinals):##constraint_type="num" or "value" or "predicate"
        self.frame_name = "OrdinalFrame"
        self.rank = temporal_ordinals.rank
        self.target = None
        if(temporal_ordinals.target):
            self.target = Event(temporal_ordinals.target)

    def template_grounding(self,parsed_question):
        graphs = []
        if (not self.target):
            self.target = Event(text=parsed_question.question)
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        graph = QueryGraph(parsed_question)
        graph.set_frame_name(self.frame_name)
        graph_list = []
        if (len(entities) == 2):
            graph_list += self.target.grounding_for_two_entities(entities[0], entities[1], graph)
        for entity in entities:
            rawgraph = graph.copy()
            graph_list += self.target.grounding(entity=entity, rawgraph=rawgraph, question=parsed_question.question, limit=canidate_num)
        for graph in graph_list:
            target_graphs = extend_graph_with_temporal_predicate(graph)
            for graph, time_var in target_graphs:
                graph.add_temporal_order(time_var, self.rank)
                graph.set_answer_vars(["s0Aspect"])
                graph.add_answer_type()
                graphs.append(graph)
        return filter_graphs(graphs)

class TAsFonWikidata(BRFonWikidata):
    def __init__(self, temporal_relation):
        self.frame_name = "TemporalAspectFrame"
        self.type = temporal_relation.rel_type
        self.target = None
        if(temporal_relation.target.type == "E3"):
            self.target = Property(temporal_relation.target,"property")
        else:
            self.target = Event(temporal_relation.target)
        self.time = temporal_relation.related.interval

    def template_grounding(self,parsed_question):####to do
        graphs = []
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        rawgraph = QueryGraph(parsed_question)
        rawgraph.set_frame_name(self.frame_name)
        graph_list = []
        for entity in entities:
            graph_list += self.target.grounding(entity, rawgraph, parsed_question.question)
        for graph in graph_list:
            target_graphs = extend_graph_with_temporal_predicate(graph,var="s0Aspect")
            for new_graph,time_var in target_graphs:
                new_graph.add_time_value_relation(time_var, TIME_VALUE_RELATION.EQUAL, self.time)
                new_graph.set_answer_vars(["s0Aspect"])
                graphs.append(new_graph)
        return graphs

class SFonWikidata(BRFonWikidata):
    def __init__(self, temporal_relation):
        self.frame_name = "SuccessivelFrame"
        self.type = temporal_relation.rel_type
        self.target = Event(temporal_relation.target, "target")
        self.related = Event(temporal_relation.related, "related")
        if ("BEFORE" in self.type):
            self.rank = -1
        else:
            self.rank = 1

    def template_grounding(self,raw_graphs):
        graphs = []
        for graph in raw_graphs:
            for edge in graph.edges:
                if (edge.type == Edge.TIME_VALUE_RELATION):
                    new_graph = graph.copy()
                    from_val = graph.find_node(edge.from_id).value
                    new_graph.add_temporal_order(from_val, self.rank)
                    graphs.append(new_graph)
        if(not self.type.startswith("I")):
            graphs += raw_graphs
        return filter_graphs(graphs)

class STFonWikidata(BRFonWikidata):# SEQ
    def template_grounding(self,parsed_question):
        graphs = []
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        target = Event(text=parsed_question.question)
        raw_graph = QueryGraph(parsed_question)
        raw_graph.set_frame_name("SequentialFrame")
        target_graphs = self.generate_onehop_bone(parsed_question)
        for graph in target_graphs:
            labels = [query_label(parse_id(edge.value)) for edge in graph.edges]
            scores = seq_ranker.rank(parsed_question.question, labels)
            if (max(scores) > 0):
                graph.set_frame_name("SequentialFrame")
                graph.add_answer_var("ans")
                graphs.append(graph)
        # for graph in target_graphs:
        #     is_seq = False
        #     for pre in list(graph.predicates):
        #         if(parse_id(pre) in temporal_relation_predicates):
        #             is_seq = True
        #     if(is_seq):
        #         graph.set_frame_name("SequentialFrame")
        #         graph.add_answer_var("ans")
        #         graphs.append(graph)
        return graphs


class TAFonWikidata(BRFonWikidata):#TemporalAnsFrame
    def __init__(self, temporal_ans_cons):
        self.frame_name = "TemporalAnsFrame"
        self.signal = temporal_ans_cons.signal.text
        self.target = None
        self.constraint = None
        if("start" in self.signal.lower()):
            self.constraint = "start"
        if("end" in self.signal.lower()):
            self.constraint = "end"

    def template_grounding(self,parsed_question):
        graphs = []
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        if(len(parsed_question.rec_numerical_cmp_cons_list)>0):
            numerical_cmp_cons = parsed_question.rec_numerical_cmp_cons_list[0]
            self.target = Property(numerical_cmp_cons.number,"num")
            for entity in entities:
                rawgraph = QueryGraph(parsed_question)
                rawgraph.set_frame_name(self.frame_name)
                graph_list = self.target.grounding(entity, rawgraph)
                for graph in graph_list:
                    graph.add_numerical_cmp("s0Aspect", numerical_cmp_cons.cmp_sign, numerical_cmp_cons.number)
                    graph.add_triple("s0", get_uri("pq:P585"), "ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)
            return graphs

        if(len(entities)==2):
            time_graphs = []
            main_paths = get_path_between_two_entity(entities[0],entities[1])
            if(len(main_paths)==0):
                entities[1], entities[0] = entities[0], entities[1]
                main_paths = get_path_between_two_entity(entities[0], entities[1])
            for main_path in main_paths:
                graph = QueryGraph(parsed_question)
                graph.set_frame_name(self.frame_name)
                graph.add_triple(entities[0], main_path[0], "s0")
                graph.add_triple("s0", main_path[1], entities[1])
                time_graphs += extend_graph_with_temporal_predicate(graph)
                graph.set_statement_type("s0")
                time_graphs += extend_graph_with_temporal_predicate(graph)
            for graph, time_var in time_graphs:
                graph.set_answer_vars([time_var])
                if(len(graph.get_answers())>0):
                    graphs.append(graph)
        self.target = Event(text=parsed_question.question, var="target")
        for entity in entities:
            rawgraph = QueryGraph(parsed_question)
            rawgraph.set_frame_name(self.frame_name)
            tmp_graphs = self.target.ground_for_temporal_ans(entity, rawgraph, question=parsed_question.question)
            for target_graph, target_var in tmp_graphs:
                graphs.append(target_graph)
            if (len(parsed_question.rec_temporal_ordinals) > 0):
                for target_graph, target_var in tmp_graphs:
                    target_graph.add_temporal_order(target_var,parsed_question.rec_temporal_ordinals[0].rank)
                    target_graph.set_answer_vars([target_var])
                    graphs.append(target_graph)
            out_paths = get_onehop_path_to_time(entity)
            out_paths += get_onehop_path_to_time_event(entity)
            for out_path in out_paths:
                graph = QueryGraph(parsed_question)
                graph.set_frame_name(self.frame_name)
                graph.add_triple(entity, out_path, "ans")
                graph.add_answer_var("ans")
                graphs.append(graph)
            in_paths = get_onehop_path_from_time_event(entity)
            for in_path in in_paths:
                graph = QueryGraph(parsed_question)
                graph.set_frame_name(self.frame_name)
                graph.add_triple("ans", in_path, entity)
                graph.add_answer_var("ans")
                graphs.append(graph)
        return graphs
