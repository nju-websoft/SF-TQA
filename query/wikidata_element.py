from utils.wikidata_utils import *
from utils.nlp_utils import spacy_text_similarity
from rank.wikidata_tag import WikidataPathRankTagger

tagger = WikidataPathRankTagger()
ranker = None
canidate_num = 5

def extend_graph_with_temporal_predicate(graph,var=None):
    time_graphs = []
    if(var):
        tmp_graph = graph.copy()
        time_var = "var" + str(tmp_graph.node_id)
        tmp_graph.add_triple(var, "p", time_var)
        tmp_graph.set_answer_vars(["p"])
        results = tmp_graph.execute()
        for result in results:
            prop = result["p"]
            if is_time_predicate(prop):
                new_graph = graph.copy()
                new_graph.add_time_interval(var, prop, time_var)
                time_graphs.append((new_graph, time_var))
    else:
        for var in graph.vars:
            tmp_graph = graph.copy()
            time_var = "var" + str(tmp_graph.node_id)
            tmp_graph.add_triple(var,"p",time_var)
            tmp_graph.set_answer_vars(["p"])
            results = tmp_graph.execute()
            for result in results:
                prop = result["p"]
                if is_time_predicate(prop):
                    new_graph = graph.copy()
                    new_graph.add_time_interval(var,prop,time_var)
                    time_graphs.append((new_graph,time_var))
            for result1 in results:
                for result2 in results:
                    prop1 = result1["p"]
                    prop2 = result2["p"]
                    if is_time_pair_predicate(prop1,prop2):
                        new_graph = graph.copy()
                        new_graph.add_time_interval(var,[prop1,prop2],time_var)
                        time_graphs.append((new_graph,time_var))
    return time_graphs

def extend_graph_with_time_and_event(graph):
    time_graphs = []
    graph.set_answer_vars(["targetpar"])
    for var in graph.vars:
        is_time_event = False
        tmp_graph = graph.copy()
        tmp_graph.add_triple(var, get_uri("p:P793"), "a")
        tmp_graph.add_triple("a", get_uri("ps:P793"), get_uri("wd:Q1190554"))
        if(len(tmp_graph.execute())):
            is_time_event = True
        tmp_graph = graph.copy()
        time_var = "var" + str(tmp_graph.node_id)
        tmp_graph.add_triple(var, "p", time_var)
        tmp_graph.set_answer_vars(["p"])
        results = tmp_graph.execute()
        for result in results:
            prop = result["p"]
            if is_time_predicate(prop):
                new_graph = graph.copy()
                new_graph.add_time_interval(var, prop, time_var)
                if (is_time_event):
                    tmp_graph = new_graph.copy()
                    time_graphs.append((tmp_graph, time_var))
                new_graph.set_answer_vars([time_var])
                time_graphs.append((new_graph, time_var))
        for result1 in results:
            for result2 in results:
                prop1 = result1["p"]
                prop2 = result2["p"]
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
    def __init__(self, event=None, var=None, interval=None,text = None):
        self.var = var
        if(text):
            self.text = text
        else:
            self.text = event.text
        self.entities = None
        self.interval = interval

    def event_with_time(self,entity,rawgraph):
        if (self.var):
            var = self.var
        else:
            var = "s0"
        time_graphs = []
        out_paths = get_onehop_path_to_time(entity)
        for out_path in out_paths:
            new_graph = rawgraph.copy()
            time_var = "var" + str(new_graph.node_id)
            new_graph.add_time_interval(var, out_path, time_var)
            time_graphs.append((new_graph, time_var))
        for out_path1 in out_paths:
            for out_path2 in out_paths:
                if is_time_pair_predicate(out_path1, out_path2):
                    new_graph = rawgraph.copy()
                    time_var = "var" + str(new_graph.node_id)
                    new_graph.add_time_interval(var, [out_path1, out_path2], time_var)
                    time_graphs.append((new_graph, time_var))
        return time_graphs

    def grounding(self, entity, rawgraph, question, limit=None):
        if(self.var):
            var = self.var
        else:
            var = "s0"
        graphs = []
        out_paths, has_statement_type = query_out_paths_for_event_statement(entity)
        for out_path in out_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity, out_path[0], var)
            graph.add_triple(var, out_path[1], var+"Aspect")
            if(has_statement_type):
                graph.set_statement_type(var)
            graphs.append(graph)
        in_paths, has_statement_type = query_in_paths_for_event_statement(entity)
        for in_path in in_paths:
            graph = rawgraph.copy()
            graph.add_triple(var+"Aspect", in_path[1], var)
            graph.add_triple(var, in_path[0], entity)
            if (has_statement_type):
                graph.set_statement_type(var)
            graphs.append(graph)
        paths = get_twohop_path_with_time_event(entity,var)
        for path in paths:
            graph = rawgraph.copy()
            graph.add_triple(path[0],path[1],path[2])
            graph.add_triple(path[3],path[4],path[5])
            graphs.append(graph)
        if limit and len(graphs) > limit:
            labels = [tagger.tag(graph) for graph in graphs]
            scores = ranker.rank(question,labels)
            for graph,score in zip(graphs,scores):
                graph.score = score
            graphs.sort(key=lambda g:g.score,reverse=True)
            graphs = graphs[:limit]
        return graphs

    def grounding_for_statement_with_time(self, entity, rawgraph, question, limit=None):
        if(self.var):
            var = self.var
        else:
            var = "s0"
        graphs = []
        out_paths, has_statement_type = query_out_paths_for_event_statement(entity)
        for out_path in out_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity, out_path[0], var)
            graph.add_triple(var, out_path[1], var+"Aspect")
            if(has_statement_type):
                graph.set_statement_type(var)
            graphs.append(graph)
        in_paths, has_statement_type = query_in_paths_for_event_statement(entity)
        for in_path in in_paths:
            graph = rawgraph.copy()
            graph.add_triple(var+"Aspect", in_path[1], var)
            graph.add_triple(var, in_path[0], entity)
            if (has_statement_type):
                graph.set_statement_type(var)
            graphs.append(graph)
        if limit and len(graphs) > limit:
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
        if (len(main_paths) == 0):
            entity1, entity2 = entity2, entity1
            main_paths = get_path_between_two_entity(entity1, entity2)
        for main_path in main_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity1, main_path[0], "s0")
            graph.add_triple("s0", main_path[1], entity2)
            graphs.append(graph)
        main_paths = get_path_from_two_entity(entity1,entity2)
        for main_path in main_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity1, main_path[0], "s0")
            graph.add_triple(entity2, main_path[1], "s0")
            graphs.append(graph)
        main_paths = get_path_to_two_entity(entity1,entity2)
        for main_path in main_paths:
            graph = rawgraph.copy()
            graph.add_triple("s0", main_path[0], entity1)
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
        in_paths = get_onehop_path_from_time_event(entity1)
        for in_path in in_paths:
            graph = rawgraph.copy()
            graph.add_triple("s0", in_path, entity1)
            graphs.append(graph)
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
                if mention in related_mention or related_mention in mention:
                    related_topics.append(link.uri)

        if len(related_topics) == 0:
            return []

        # temporal entity first
        for entity in related_topics:
            out_paths = query_onehop_out_paths(entity)
            out_paths = [x[0] for x in out_paths]
            if get_uri("p:P580") in out_paths and get_uri("p:P582") in out_paths:
                graph = rawgraph.copy()
                graph.add_time_interval(entity, [get_uri("wdt:P580"), get_uri("wdt:P582")], "related_time")
                return [(graph, "related_time")]
            elif get_uri("p:P793") in out_paths:
                graph = rawgraph.copy()
                graph.add_triple(entity, get_uri("p:P793"), "related_stmt")
                graph.add_time_interval("related_stmt", [get_uri("pq:P580"), get_uri("pq:P582")], "related_time")
                return [(graph, "related_time")]

        # retrieve related event stmt
        graphs = []
        for related_entity in related_topics:
            out_paths = query_temporal_onehop_out_paths(related_entity)
            for out_path in out_paths:
                if check_prefix(out_path[1], "ps"):
                    graph = rawgraph.copy()
                    graph.add_triple(related_entity, out_path[0], "related_var1")
                    graph.add_time_interval("related_var1", out_path[1], "related_time")
                    graphs.append(graph)
            for out_path1 in out_paths:
                for out_path2 in out_paths:
                    if is_time_pair_predicate(out_path1[0], out_path2[0]):
                        prop1 = get_uri("wdt:" + parse_id(out_path1[0]))
                        prop2 = get_uri("wdt:" + parse_id(out_path2[0]))
                        graph = rawgraph.copy()
                        graph.add_time_interval(related_entity, [prop1, prop2], "related_time")
                        graphs.append(graph)

        links = parsed_question.entity_linking.values()
        entities = [link.uri for link in links]
        for related_entity in related_topics:
            for entity in entities:
                if entity != related_entity:
                    preds = query_stmts(related_entity, entity)
                    if len(preds) > 0:
                        for pred in preds:
                            p_pred = get_uri("p:" + pred)
                            ps_pred = get_uri("ps:" + pred)
                            tmp_graph = rawgraph.copy()
                            tmp_graph.add_triple(related_entity, p_pred, "related_stmt")
                            tmp_graph.add_triple("related_stmt", ps_pred, entity)
                            stmt_time_qualifiers = query_stmt_time_qualifiers(pred, related_entity, entity)
                            for qualifier in stmt_time_qualifiers:
                                related_graph = tmp_graph.copy()
                                related_graph.add_time_interval("related_stmt", qualifier, "related_time")
                                graphs.append(related_graph)
                            for q1 in stmt_time_qualifiers:
                                for q2 in stmt_time_qualifiers:
                                    if is_time_pair_predicate(q1, q2):
                                        related_graph = tmp_graph.copy()
                                        related_graph.add_time_interval("related_stmt", [q1, q2], "related_time")
                                        graphs.append(related_graph)

                    preds = query_stmts(entity, related_entity)
                    if len(preds) > 0:
                        for pred in preds:
                            p_pred = get_uri("p:" + pred)
                            ps_pred = get_uri("ps:" + pred)
                            tmp_graph = rawgraph.copy()
                            tmp_graph.add_triple(entity, p_pred, "related_stmt")
                            tmp_graph.add_triple("related_stmt", ps_pred, related_entity)
                            stmt_time_qualifiers = query_stmt_time_qualifiers(pred, entity, related_entity)
                            for qualifier in stmt_time_qualifiers:
                                related_graph = tmp_graph.copy()
                                related_graph.add_time_interval("related_stmt", qualifier, "related_time")
                                graphs.append(related_graph)
                            for q1 in stmt_time_qualifiers:
                                for q2 in stmt_time_qualifiers:
                                    if is_time_pair_predicate(q1, q2):
                                        related_graph = tmp_graph.copy()
                                        related_graph.add_time_interval("related_stmt", [q1, q2], "related_time")
                                        graphs.append(related_graph)
            entities.remove(related_entity)

        # rank related graph
        # print(len(graphs))
        if len(graphs) > limit:
            labels = []
            for graph in graphs:
                label = tagger.tag(graph)
                label_tokens = [token for token in label.lower().split() if token[0] != "["]
                label = " ".join(set(label_tokens))
                labels.append(label)
            graphs_with_score = [(graph, spacy_text_similarity(question, label)) for graph, label in
                                 zip(graphs, labels)]
            graphs_with_score.sort(key=lambda x: x[1], reverse=True)
            graphs_with_score = graphs_with_score[:limit]
            graphs = [graph for graph, _ in graphs_with_score]
        graphs_with_time = [(graph, "related_time") for graph in graphs]
        return graphs_with_time

    def grounding_with_time(self, entity, rawgraph, question):
        graphs = []
        rawgraphs = self.grounding(entity,rawgraph, question=question,limit=canidate_num)
        for graph in rawgraphs:
            graphs += extend_graph_with_temporal_predicate(graph)
        return graphs

    def ground_for_temporal_ans(self,entity, rawgraph, question):
        graphs = []
        rawgraphs = self.grounding(entity, rawgraph, question=question)
        for graph in rawgraphs:
            graphs += extend_graph_with_time_and_event(graph)
        return graphs

class Event_for_nominal():
    def __init__(self, event, var=None, interval=None):
        self.nlinfo = event
        self.type = event.type
        self.var = var
        self.text = event.text
        self.entity = None
        self.interval = interval

class Property():
    def __init__(self,vlaue,name,var=None):
        self.name = None
        self.time = None
        self.num = None
        self.var = var
        if (name == "num"):
            self.num = vlaue
        if(name == "property"):
            self.name = vlaue.text

    def grounding(self, entity, rawgraph, question=None, limit=None):
        if (self.var):
            var = self.var
        else:
            var = "s0"
        graphs = []
        out_paths = get_statement_path_to_value(entity)
        for out_path in out_paths:
            graph = rawgraph.copy()
            graph.add_triple(entity, out_path[0], "s0")
            graph.add_triple("s0", out_path[1], var+"Aspect")#"num"
            graphs.append(graph)
        if limit and len(graphs) > limit:
            labels = [tagger.tag(graph) for graph in graphs]
            scores = ranker.rank(question,labels)
            for graph,score in zip(graphs,scores):
                graph.score = score
            graphs.sort(key=lambda g:g.score,reverse=True)
            graphs = graphs[:limit]
        return graphs

class OrdinalQueryGraph():
    def __init__(self,graph,rank): #s pred entity
        self.base_query = graph
        self.rank = rank
        self.frame_name = "OrdinalSeqFrame"
        for e in list(graph.entities):
            if(e.startswith('http://www.wikidata.org/entity/Q')):
                self.entity = e
        edges = graph.get_node_out_edges(name="s0")
        for edge in edges:
            if(edge.value.startswith("http://www.wikidata.org/prop/qualifier/P")):
                self.qualifier_pred = edge.value
            elif(edge.value.startswith("http://www.wikidata.org/prop/statement/P")):
                self.pred_to_entity = edge.value

    def generate_ord_list(self):
        res = query_wikidata_with_odbc(self.base_query.to_sparql())
        entity_list = []
        if (len(res) == 1):
            entity_list.append(res[0]["ans"])
            entity_list.append(res[0]["s0Aspect"])
            return entity_list
        entity_map = {}
        e1_list = set()
        e2_list = set()
        # e1 replaces e2 : e2->e1
        for tuple in res:
            e1 = tuple["s0Aspect"]
            e2 = tuple["ans"]
            e1_list.add(e1)
            e2_list.add(e2)
            if(e1 in entity_map):
                entity_map[e1].append([e2,0])
            else:
                entity_map[e1] = [[e2,0]]
        diff1 = list(e1_list.difference(e2_list))
        diif2 = list(e2_list.difference(e1_list))
        e_res = []
        for diff in diff1:
            if(len(entity_map[diff])==1 and entity_map[diff][0][0] in diif2):
                continue
            e_res.append(diff)
        e = e_res[0]
        while (e in entity_map):
            entity_list.append(e)
            for next_e in entity_map[e]:
                if(next_e[1]==0):
                    next_e[1] = 1
                    entity_list.insert(0, next_e[0])
                    e = next_e[0]
                    break
        return entity_list

    def get_answers(self):
        global ord_seq_list
        label = self.pred_to_entity +" to "+self.entity
        if(label in ord_seq_list):
            if(len(ord_seq_list[label]) > self.rank):
                return [ord_seq_list[label].list[self.rank-1]]
            else:
                return []
        ord_seq_list[label] = self.generate_ord_list()
        return [ord_seq_list[label][self.rank-1]]



