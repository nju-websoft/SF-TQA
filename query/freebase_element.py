from utils.freebase_utils import *
from utils.nlp_utils import spacy_text_similarity
from rank.freebase_tag import FreebasePathRankTagger

ranker = None
tagger = FreebasePathRankTagger()
canidate_num = 10

def extend_graph_with_temporal_predicate(graph,var=None):
    time_graphs = []
    if(var):
        if(var in graph.vars):
            raw_ans_vars = graph.answer_vars
            graph.set_answer_vars(["p"])
            time_var = "var" + str(graph.node_id)
            candicate_time_predicates = get_time_predicates_from_var(var, graph.to_sparql())
            graph.set_answer_vars(raw_ans_vars)
            for prop in candicate_time_predicates:
                new_graph = graph.copy()
                new_graph.add_time_interval(var, prop, time_var)
                time_graphs.append((new_graph, time_var))
    else:
        for var in graph.vars:
            raw_ans_vars = graph.answer_vars
            graph.set_answer_vars(["p"])
            time_var = "var" + str(graph.node_id)
            candicate_time_predicates = get_time_predicates_from_var(var,graph.to_sparql())
            graph.set_answer_vars(raw_ans_vars)
            for prop in candicate_time_predicates:
                new_graph = graph.copy()
                new_graph.add_time_interval(var, prop, time_var)
                time_graphs.append((new_graph, time_var))
            for prop1 in candicate_time_predicates:
                for prop2 in candicate_time_predicates:
                    if is_time_pair_predicate(prop1, prop2):
                        new_graph = graph.copy()
                        new_graph.add_time_interval(var, [prop1, prop2], time_var)
                        time_graphs.append((new_graph, time_var))
    return time_graphs

def extend_graph_with_time_and_event(graph):
    time_graphs = []
    for var in graph.vars:
        is_time_event = False
        tmp_graph = graph.copy()
        tmp_graph.add_sparql_triple(var, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://rdf.freebase.com/ns/time.event>")
        if(len(tmp_graph.execute())):
            is_time_event = True
        raw_ans_vars = graph.answer_vars
        graph.set_answer_vars(["p"])
        time_var = "var" + str(graph.node_id)
        candicate_time_predicates = get_time_predicates_from_var(var,graph.to_sparql())
        graph.set_answer_vars(raw_ans_vars)
        for prop in candicate_time_predicates:
            new_graph = graph.copy()
            new_graph.add_time_interval(var, prop, time_var)
            if (is_time_event):
                tmp_graph = new_graph.copy()
                time_graphs.append((tmp_graph, time_var))
            new_graph.set_answer_vars([time_var])
            time_graphs.append((new_graph, time_var))
        for prop1 in candicate_time_predicates:
            for prop2 in candicate_time_predicates:
                if is_time_pair_predicate(prop1, prop2):
                    new_graph = graph.copy()
                    new_graph.add_time_interval(var, [prop1, prop2], time_var)
                    if (is_time_event):
                        tmp_graph = new_graph.copy()
                        time_graphs.append((tmp_graph, time_var))
                    new_graph.set_answer_vars([time_var])
                    time_graphs.append((new_graph, time_var))
    return time_graphs

class Event():
    def __init__(self, event=None, var=None, interval=None, text=None):
        self.var = var
        if(text):
            self.text = text
        else:
            self.text = event.text
        self.entities = None
        self.interval = interval

    def grounding(self, entity, rawgraph, question, limit=canidate_num):
        graphs = []
        out_paths = query_one_or_twohop_out_paths_for_event(entity)
        for out_path in out_paths:
            if not out_path[0].startswith("http"):
                print(out_path)
            if len(out_path) == 1:
                graph = rawgraph.copy()
                graph.add_triple(entity, out_path[0], "ans")
                graph.add_answer_var("ans")
                graphs.append(graph)
            else:
                graph = rawgraph.copy()
                graph.add_triple(entity, out_path[0], "target")
                graph.add_triple("target", out_path[1], "ans")
                graph.add_answer_var("ans")
                graphs.append(graph)
        if limit > 0 and len(graphs) > limit:
            labels = [tagger.tag(graph) for graph in graphs]
            scores = ranker.rank(question,labels)
            for graph,score in zip(graphs,scores):
                graph.score = score
            graphs.sort(key=lambda g:g.score,reverse=True)
            graphs = graphs[:limit]
        return graphs

    def grounding_for_two_entities(self, entity1, entity2, rawgraph):
        graphs = []
        main_paths = get_path_between_two_entity(entity1, entity2)
        for main_path in main_paths:
            graph = rawgraph.copy()
            if(len(main_path)==2):
                graph.add_triple(entity1, main_path[0], "s0")
                graph.add_triple("s0", main_path[1], entity2)
                graphs.append(graph)
        return graphs

    def grounding_to_time_event(self, entity1, rawgraph):
        graphs = []
        out_paths = get_onehop_path_to_time_event(entity1)
        for main_path in out_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity1, main_path, "s0")
            graphs.append(graph)
        return graphs

    def ground_for_temporal_ans(self,entity, rawgraph, question):
        graphs = []
        rawgraphs = self.grounding(entity, rawgraph, question)
        for graph in rawgraphs:
            graphs += extend_graph_with_time_and_event(graph)
        return graphs

    def grounding_with_time(self, entity, rawgraph, question):
        graphs = []
        rawgraphs = self.grounding(entity, rawgraph, question)
        for graph in rawgraphs:
            graphs += extend_graph_with_temporal_predicate(graph)
        return graphs

    def grounding_for_related_event_with_time(self, parsed_question, rawgraph, limit=canidate_num):
        # get related topic entity
        related_topics = []
        related_mention = None
        question = parsed_question.question
        entity_linking = parsed_question.entity_linking
        trigger_words = [" after ", " when ", " before ", " during ", " while ", " in ", " at "]

        # extract related mention
        for trigger in trigger_words:
            if trigger in question:
                index = question.find(trigger)
                related_mention = question[index + len(trigger):]
                break

        if related_mention is not None:
            for mention, link in entity_linking.items():
                if mention in related_mention:
                    related_topics.append(link.uri)

        if len(related_topics) == 0:
            return []

        # entity represent period
        for related_topic in related_topics:
            out_paths = query_onehop_out_paths(related_topic)
            if "http://rdf.freebase.com/ns/time.event.start_date" in out_paths \
                    and "http://rdf.freebase.com/ns/time.event.end_date" in out_paths:
                graph = rawgraph.copy()
                graph.add_time_interval(related_topic, ["http://rdf.freebase.com/ns/time.event.start_date",
                                                        "http://rdf.freebase.com/ns/time.event.end_date"],
                                        "related_time")
                return [(graph, "related_time")]

        # retrieve path
        graphs = []
        for related_topic in related_topics:
            out_paths = query_onehop_out_paths(related_topic)
            out_paths = [out_path for out_path in out_paths if is_time_predicate(out_path)]
            for out_path in out_paths:
                graph = rawgraph.copy()
                graph.add_time_interval(related_topic, out_path, "related_time")
                graphs.append(graph)
            for out_path1 in out_paths:
                for out_path2 in out_paths:
                    if is_time_pair_predicate(out_path1, out_path2):
                        graph = rawgraph.copy()
                        graph.add_time_interval(related_topic, [out_path1, out_path2], "related_time")
                        graphs.append(graph)

        links = parsed_question.entity_linking.values()
        entities = [link.uri for link in links]
        for related_entity in related_topics:
            for entity in entities:
                if entity != related_entity:
                    preds = query_relation_by_cvt(related_entity, entity)
                    if len(preds) > 0:
                        for prop1, prop2 in preds:
                            tmp_graph = rawgraph.copy()
                            tmp_graph.add_triple(related_entity, prop1, "related_cvt")
                            tmp_graph.add_triple("related_cvt", prop2, entity)
                            cvt_time_preds = query_cvt_time_preds(prop1, prop2, related_entity, entity)
                            for time_pred in cvt_time_preds:
                                related_graph = tmp_graph.copy()
                                related_graph.add_time_interval("related_cvt", time_pred, "related_time")
                                graphs.append(related_graph)
                            for time_pred1 in cvt_time_preds:
                                for time_pred2 in cvt_time_preds:
                                    if is_time_pair_predicate(time_pred1, time_pred2):
                                        related_graph = tmp_graph.copy()
                                        related_graph.add_time_interval("related_cvt", [time_pred1, time_pred2],
                                                                        "related_time")
                                        graphs.append(related_graph)

                    preds = query_relation_by_cvt(entity, related_entity)
                    if len(preds) > 0:
                        for prop1, prop2 in preds:
                            tmp_graph = rawgraph.copy()
                            tmp_graph.add_triple(entity, prop1, "related_cvt")
                            tmp_graph.add_triple("related_cvt", prop2, related_entity)
                            cvt_time_preds = query_cvt_time_preds(prop1, prop2, entity, related_entity)
                            for time_pred in cvt_time_preds:
                                related_graph = tmp_graph.copy()
                                related_graph.add_time_interval("related_cvt", time_pred, "related_time")
                                graphs.append(related_graph)
                            for time_pred1 in cvt_time_preds:
                                for time_pred2 in cvt_time_preds:
                                    if is_time_pair_predicate(time_pred1, time_pred2):
                                        related_graph = tmp_graph.copy()
                                        related_graph.add_time_interval("related_cvt", [time_pred1, time_pred2],
                                                                        "related_time")
                                        graphs.append(related_graph)
            entities.remove(related_entity)

        # rank related graph
        labels = []
        for graph in graphs:
            label = tagger.tag(graph)
            label_tokens = [token for token in label.lower().split() if token[0] != "["]
            label = " ".join(set(label_tokens))
            labels.append(label)
        # event_mention = " ".join(set((related_mention + " " + temporal_relation.related.text).split()))
        graphs_with_score = [(graph, spacy_text_similarity(question, label)) for graph, label in zip(graphs, labels)]
        graphs_with_score.sort(key=lambda x: x[1], reverse=True)
        graphs_with_score = graphs_with_score[:limit]
        graphs_with_time = [(graph, "related_time") for graph, score in graphs_with_score]
        return graphs_with_time

class Property():
    def __init__(self,vlaue,name):
        self.name = None
        self.time = None
        self.num = None
        self.var = None
        if (name == "num"):
            self.num = vlaue
        if(name == "property"):
            self.name = vlaue.text

    def grounding(self, entity, rawgraph, question=None, limit=None):
        graphs = []
        out_paths = get_statement_path_to_value(entity)
        for out_path in out_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity, out_path, "s0")
            graph.add_triple("s0", "ns:measurement_unit.dated_integer.number", "ans")
            graphs.append(graph)
        return graphs

class TemporalEntity():
    def __init__(self,event):
        self.nlinfo = event
        self.entity_name = event.text
        self.entity = None
        if(event.interval):
            self.interval = event.interval