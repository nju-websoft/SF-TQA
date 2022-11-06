from utils.prefix_utils import check_prefix
from enum import Enum
import utils.wikidata_utils as wd
import utils.freebase_utils as fb
from utils.dataset_utils import DATASET
TIME_VALUE_RELATION = Enum('TIME_VALUE_RELATION', 'BEGIN END MEET MET_BY AFTER BEFORE OVERLAP EQUAL')

def get_year(result):
    index = result.find("-")
    return result[:index]

class Node():
    URI = 1
    VAR = 2
    LITERAL = 3
    TIME_INTERVAL = 4
    TIME_VAR = 5
    def __init__(self,nid,type,value):
        self.id = nid
        #"URI","VAR","STMT_VAR","LITERAL","TIME_INTERVAL","TIME_VAR"
        #value
        #   LITERAL:[value,type]
        #   TIME_INTERVAL:[start,end]
        self.type = type
        self.value = value
    
    def is_name_node(self):
        if self.type == Node.URI or self.type == Node.VAR or self.type == Node.TIME_VAR:
            return True
        return False

class Edge():
    URI = 1
    VAR = 2
    AGG = 3
    CMP = 4
    TIME_VALUE_PREDICATE = 5
    TIME_VALUE_RELATION = 6
    TEMPORAL_ORDER = 7
    NUMERICAL_CMP = 8
    ITERATION_ORDER = 9
    def __init__(self,from_id,to_id,value=None,edge_type=None):
        self.from_id = from_id
        self.to_id = to_id
        self.value = value
        self.type = edge_type

class QueryGraph():
    def __init__(self,parsed_question=None):
        self.parsed_question = parsed_question
        self.nodes = []
        self.edges = []
        self.name2node = {}
        self.id2node = {}
        self.entities = set()
        self.predicates = set()
        self.vars = set()
        self.answer_vars = set()
        self.node_id = 0
        self.score = 0.0
        self.isentity = False
        self.frame_name = None
        if self.parsed_question.ground_kb == "WIKIDATA":
            self.tagger = QueryGraphWikidataTagger()
        else:
            self.tagger = QueryGraphFreebaseTagger()

    def set_frame_name(self,frame_name):
        self.frame_name = frame_name

    def is_contains(self,cmp_graph):
        for entity in list(cmp_graph.entities):
            if(entity not in self.entities):
                return False
        for predicate in list(cmp_graph.predicates):
            if(predicate not in self.predicates):
                return False
        return True
    
    def add_sparql_triple(self,from_val,edge_val,to_val):
        from_val = from_val.strip("<").strip(">").strip(".")
        edge_val = edge_val.strip("<").strip(">").strip(".")
        to_val = to_val.strip("<").strip(">").strip(".")
        
        if from_val.startswith("?"):
            from_node = self.add_node(Node.VAR,from_val[1:])
        else:
            from_node = self.add_node(Node.URI,from_val)
        
        if to_val.startswith("?"):
            to_node = self.add_node(Node.VAR,to_val[1:])
        else:
            to_node = self.add_node(Node.URI,to_val)

        if edge_val.startswith("?"):
            edge = Edge(from_node.id,to_node.id,edge_val,Edge.VAR)
        else:
            edge = Edge(from_node.id,to_node.id,edge_val,Edge.URI)
            self.predicates.add(edge_val)

        self.edges.append(edge)
        return self

    def set_statement_type(self,val):
        from_node = self.find_node(name=val)
        to_node = self.add_node(Node.URI, "http://wikiba.se/ontology#Statement")
        self.edges.append(Edge(from_node.id, to_node.id, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", Edge.URI))
        self.predicates.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        return self
    
    def add_triple(self,from_val,edge_val,to_val):
        if from_val in self.name2node.keys():
            from_node = self.name2node[from_val]
        else:
            if from_val.startswith("http"):
                from_node = self.add_node(Node.URI,from_val)
            else:
                from_node = self.add_node(Node.VAR,from_val)
        
        if to_val in self.name2node.keys():
            to_node = self.name2node[to_val]
        else:
            if to_val.startswith("http"):
                to_node = self.add_node(Node.URI,to_val)
            else:
                to_node = self.add_node(Node.VAR,to_val)

        if edge_val.startswith("http"):
            edge = Edge(from_node.id,to_node.id,edge_val,Edge.URI)
            self.predicates.add(edge_val)
        else:
            edge = Edge(from_node.id,to_node.id,edge_val,Edge.VAR)
        self.edges.append(edge)
        return self.edges

    #add datetime predicate
    def add_time_interval(self,from_val,edge_val,to_val):
        if from_val in self.name2node.keys():
            from_node = self.name2node[from_val]
        else:
            if from_val.startswith("http"):
                from_node = self.add_node(Node.URI,from_val)
            else:
                from_node = self.add_node(Node.VAR,from_val)
            
        if to_val in self.name2node.keys():
            to_node = self.name2node[to_val]
        else:
            to_node = self.add_node(Node.TIME_VAR,to_val)
        
        edge = Edge(from_node.id,to_node.id,edge_val,Edge.TIME_VALUE_PREDICATE)
        self.edges.append(edge)
        return self
    
    def add_time_value_relation(self,from_val,edge_val,to_val):
        if from_val in self.name2node.keys():
            from_node = self.name2node[from_val]
        else:
            from_node = self.add_node(Node.VAR,from_val)

        if type(to_val) == type([]):
            to_node = self.add_node(Node.TIME_INTERVAL,to_val)
        else:
            if to_val in self.name2node.keys():
                to_node = self.name2node[to_val]
            else:
                to_node = self.add_node(Node.TIME_VAR,to_val)
        
        edge = Edge(from_node.id,to_node.id,edge_val,Edge.TIME_VALUE_RELATION)
        self.edges.append(edge)
        return self
    
    def add_temporal_order(self,var,rank):
        if var in self.name2node.keys():
            from_node = self.name2node[var]
        else:
            from_node = self.add_node(Node.VAR,var)
        to_node = self.add_node(Node.LITERAL,[rank,None])
        edge = Edge(from_node.id,to_node.id,"TEMPORAL_ORDER",Edge.TEMPORAL_ORDER)
        self.edges.append(edge)
        return self
    
    def add_numerical_cmp(self,var,cmp_sign,number):
        if var in self.name2node.keys():
            from_node = self.name2node[var]
        else:
            from_node = self.add_node(Node.VAR,var)
        to_node = self.add_node(Node.LITERAL,[str(number),None])
        edge = Edge(from_node.id,to_node.id,cmp_sign,Edge.NUMERICAL_CMP)
        self.edges.append(edge)
        return self
    
    def get_node_in_edges(self,id=None,name=None):
        if name is not None and name in self.name2node.keys():
            node = self.name2node[name]
        elif id is not None and id in self.id2node.keys():
            node = self.id2node[id]
        else:
            node = None
        if node is None:
            return []
        in_edges = []
        for edge in self.edges:
            tonode = self.id2node[edge.to_id]
            if tonode == node:
                in_edges.append(edge)
        return in_edges

    def get_node_out_edges(self,id=None,name=None):
        if name is not None:
            node = self.name2node[name]
        elif id is not None:
            node = self.id2node[id]
        else:
            node = None
        if node is None:
            return []
        out_edges = []
        for edge in self.edges:
            from_node = self.id2node[edge.from_id]
            if from_node == node:
                out_edges.append(edge)
        return out_edges

    def check_stmt_var(self,id=None,name=None):
        in_edges = self.get_node_in_edges(id,name)
        for edge in in_edges:
            if edge.type == Edge.URI and check_prefix(edge.value,"p"):
                return True
        out_edges = self.get_node_out_edges(id,name)
        for edge in out_edges:
            if edge.type == Edge.URI and (check_prefix(edge.value,"pq") or check_prefix(edge.value,"ps")):
                return True
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type("") and (check_prefix(edge.value,"pq") or check_prefix(edge.value,"ps")):
                    return True
                elif (check_prefix(edge.value[0],"pq") or check_prefix(edge.value[0],"ps")):
                    return True
        return False

    def add_node(self,ntype,nval):
        if ntype == Node.TIME_VAR or ntype == Node.VAR or ntype == Node.URI:
            if nval in self.name2node.keys():
                return self.name2node[nval]

        node = Node(self.node_id,ntype,nval)
        self.nodes.append(node)

        if ntype == Node.URI:
            self.entities.add(nval)
        elif ntype == Node.VAR:
            self.vars.add(nval)

        if node.is_name_node():
            self.name2node[nval] = node
        self.id2node[self.node_id] = node
        self.node_id += 1
        return node
    
    def find_node(self,id=None,name=None):
        if name is None and id is None:
            return None
        for node in self.nodes:
            if name is not None and node.value == name:
                return node
            if id is not None and node.id == id:
                return node
        return None
    
    def add_edge(self,from_val,from_type,edge_val,edge_type,to_val,to_type):
        from_node = self.add_node(from_type,from_val)
        to_node = self.add_node(to_type,to_val)
        edge = Edge(from_node.id,to_node.id,edge_val,edge_type)
        self.edges.append(edge)
        return self
    
    def remove_edge(self,edge):
        delete_edge = None
        for graph_edge in self.edges:
            if graph_edge.from_id == edge.from_id and graph_edge.to_id == edge.to_id:
                delete_edge = graph_edge
                break
        if delete_edge is not None:
            self.edges.remove(delete_edge)
        return self
    
    def remove_node(self,id=None,name=None):
        delete_node = self.find_node(id,name)
        if delete_node is not None:
            self.nodes.remove(delete_node)
            if delete_node.is_name_node():
                del self.name2node[delete_node.value]
            if delete_node.type == Node.VAR:
                self.vars.remove(delete_node.value)
            if delete_node.type == Node.URI:
                self.entities.remove(delete_node.value)
            del self.id2node[delete_node.id]
        return self

    def add_answer_type(self):
        self.isentity = True

    def add_answer_var(self,var):
        self.answer_vars.add(var)
    
    def set_answer_vars(self,vars):
        self.answer_vars = set(vars)
    
    def copy(self):
        querygraph = QueryGraph(self.parsed_question)
        querygraph.frame_name = self.frame_name
        querygraph.nodes = list(self.nodes)
        querygraph.edges = list(self.edges)
        querygraph.name2node = dict(self.name2node)
        querygraph.id2node = dict(self.id2node)
        querygraph.node_id = self.node_id
        querygraph.answer_vars = set(self.answer_vars)
        querygraph.entities = set(self.entities)
        querygraph.vars = set(self.vars)
        querygraph.score = self.score
        return querygraph
    
    def to_sparql(self):
        if self.parsed_question.ground_kb == "WIKIDATA":
            grounder = QueryGraphWikidataGrounder(self)
            sparql = grounder.ground()
        else:
            grounder = QueryGraphFreebaseGrounder(self)
            sparql = grounder.ground()
        return sparql
    
    def execute(self):
        sparql = self.to_sparql()
        # print(sparql)
        query_labels = str(" ".join(list(self.answer_vars)))+" "+self.tagger.tag(self)
        if self.parsed_question.ground_kb == "WIKIDATA":
            if(query_labels in wd.res_cache.keys()):
                return wd.res_cache[query_labels]
            if(query_labels in wd.res_cache_add.keys()):
                return wd.res_cache_add[query_labels]
            results = wd.query_wikidata_with_odbc(sparql)
            wd.res_cache_add[query_labels] = results
        else:
            # if(query_labels in fb.res_cache.keys()):
            #     return fb.res_cache[query_labels]
            results = fb.query_freebase(sparql)
            # fb.res_cache_add[query_labels] = results
        return results
    
    def get_answers(self):
        if self.parsed_question.dataset_type == DATASET.TQ:
            results = self.execute()
            answers = []
            for result in results:
                answers += result.values()
            try:
                answers = [answer.replace("http://www.wikidata.org/entity/","") for answer in answers]
                answers = ["-0" + answer[1:] if answer.startswith("-") and len(answer.split("-")[1]) < 3 and "T00" in answer else answer for answer in answers]
            except:
                print("L")
        elif self.parsed_question.dataset_type == DATASET.CRQ:
            results = self.execute()
            answers = []
            answer_var = next(iter(self.answer_vars))
            in_edges = self.get_node_in_edges(name=answer_var)
            if len(in_edges) > 0 and in_edges[0].type == Edge.TIME_VALUE_PREDICATE and type(in_edges[0].value) == type(
                []):
                for result in results:
                    duration = sorted(list(result.values()))
                    start = get_year(duration[0])
                    end = get_year(duration[1])
                    for i in range(int(start),int(end)):
                        answers.append(str(i))
            else:
                for result in results:
                    answers += result.values()
                for i, answer in enumerate(answers):
                    if(answer.startswith("http://www.wikidata.org/entity/")):
                        answers[i] = answer.replace("http://www.wikidata.org/entity/", "")
                    else:
                        answers[i] = get_year(answer)
                        if(self.to_sparql().find("P582")!=-1):
                            # pass
                            answers[i] = str(int(answers[i])-1)
        elif self.parsed_question.dataset_type == DATASET.TEQ:
            results = self.execute()
            answers = []
            for result in results:
                answers += result.values()
            answers = [fb.query_label(answer).lower() if answer.startswith("http") else answer.lower() for answer in answers]
            answers = [answer for answer in answers if not answer == '']
            return answers
        answers = list(set(answers))
        return answers
    
    def to_label_seq(self):
        label = self.tagger.tag(self)
        return label 

    def replace_variable(self,raw_val,new_val):
        for node in self.nodes:
            if(node.value==raw_val):
                node.value = new_val
        for edge in self.edges:
            if(edge.from_val==raw_val):
                edge.from_val = new_val
            if(edge.to_val==raw_val):
                edge.to_val = new_val

def merge_graph(graph1,graph2):
    if(graph1.to_label_seq() == graph2.to_label_seq()):
        return graph1
    graph = graph1.copy()
    answer_var = list(graph1.answer_vars)[0]
    if(answer_var==list(graph2.answer_vars)[0]):
        merge_node_val = list(graph1.answer_vars)[0]
    else:
        merge_node_val = list(graph2.answer_vars)[0]
    index = graph1.nodes[-1].id+1
    id_map = {}
    for node in graph2.nodes:
        if(node.type==2 or node.type==5):
            if(node.value == merge_node_val):
                id_map[node.id] = graph1.find_node(name=answer_var).id
            else:
                index = graph.nodes[-1].id+1
                id_map[node.id] = index
                graph.add_node(node.type, "var"+str(index))
        elif(node.type == Node.URI and graph.find_node(name=node.value)):
            id_map[node.id] = graph.find_node(name=node.value).id
        else:
            index = graph.nodes[-1].id + 1
            id_map[node.id] = index
            graph.add_node(node.type, node.value)
    for edge in graph2.edges:
        new_edge = Edge(id_map.get(edge.from_id,edge.from_id), id_map[edge.to_id], edge.value, edge.type)
        graph.edges.append(new_edge)
    return graph

class QueryGraphFreebaseGrounder():
    def __init__(self,graph):
        self.graph = graph
    
    def ground(self):
        sparql = "SELECT DISTINCT "

        #make time var instance
        time_var_map = {}
        time_var_no = 0
        for edge in self.graph.edges:
            if edge.type == Edge.TIME_VALUE_PREDICATE:
                to_node = self.graph.find_node(edge.to_id)
                var = to_node.value
                if type(edge.value) == type(""):
                    time_var_map[var] = "time" + str(time_var_no)
                else:
                    start_var = "start" + str(time_var_no)
                    end_var = "end" + str(time_var_no)
                    time_var_map[var] = [start_var,end_var]
                time_var_no += 1

        #generate select answer vars
        for var in self.graph.answer_vars:
            if var in time_var_map.keys():
                if type(time_var_map[var]) == type(""):
                    sparql += "?" + time_var_map[var] + " "
                else:
                    sparql += "?" + time_var_map[var][0] + " "
                    sparql += "?" + time_var_map[var][1] + " "
            else:
                sparql += "?" + var + " "

        #generate where clause
        sparql += "WHERE{ "
        for edge in self.graph.edges:
            if edge.type == Edge.URI or edge.type == Edge.VAR:
                triple = ""
                from_node = self.graph.find_node(edge.from_id)
                if from_node.type == Node.URI:
                    triple += "<" + from_node.value + ">"
                else:
                    triple += "?" + from_node.value
                triple += " "
                if edge.type == Edge.URI:
                    triple += "<" + edge.value + ">"
                else:
                    triple += "?" + edge.value
                triple += " "
                to_node = self.graph.find_node(edge.to_id)
                if to_node.type == Node.URI:
                    triple += "<" + to_node.value + ">"
                else:
                    triple += "?" + to_node.value
                triple += ". "
                sparql += triple
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                from_node = self.graph.find_node(edge.from_id)
                if from_node.type == Node.URI:
                    from_term = "<" + from_node.value + ">"
                else:
                    from_term = "?" + from_node.value
                to_node = self.graph.find_node(edge.to_id)
                to_node_var = to_node.value
                if type(time_var_map[to_node_var]) == type(""):
                    sparql += from_term + " <" + edge.value + "> " + "?" + time_var_map[to_node_var] + ". "
                else:
                    sparql += from_term + " <" + edge.value[0] + "> " + "?" + time_var_map[to_node_var][0] + ". "
                    sparql += from_term + " <" + edge.value[1] + "> " + "?" + time_var_map[to_node_var][1] + ". "
        
        #generate temporal filter
        for edge in self.graph.edges:
            if edge.type == Edge.TIME_VALUE_RELATION:
                from_node = self.graph.find_node(edge.from_id)
                to_node = self.graph.find_node(edge.to_id)
                if to_node.type == Node.TIME_INTERVAL:
                    filter_expr = self.make_temporal_filter_explicit(time_var_map[from_node.value],edge.value,to_node.value)
                else:
                    filter_expr = self.make_temporal_filter_implicit(time_var_map[from_node.value],edge.value,time_var_map[to_node.value])
                sparql += filter_expr
        
        #generate numerical cmp
        for edge in self.graph.edges:
            if edge.type == Edge.NUMERICAL_CMP:
                var = self.graph.find_node(edge.from_id).value
                cmp_sign = edge.value
                number = self.graph.find_node(edge.to_id).value[0]
                filter_expr = self.make_numeric_cmp_filter(var,cmp_sign,number)
                sparql += filter_expr
        sparql += "}"
        
        #generate temporal order
        for edge in self.graph.edges:
            if edge.type == Edge.TEMPORAL_ORDER:
                from_node = self.graph.find_node(edge.from_id)
                rank = self.graph.find_node(edge.to_id).value[0]
                if type(time_var_map[from_node.value]) != type(""):
                    time_var = time_var_map[from_node.value][0]
                else:
                    time_var = time_var_map[from_node.value]
                order_expr = self.make_temporal_order(time_var,rank)
                sparql += order_expr
                break

        return sparql
    
    def make_temporal_filter_explicit(self,var,temporal_relation,time_value):
        start_value = time_value[0]
        end_value = time_value[1]
        if type(var) == type(""):
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = {}".format(var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "xsd:dateTime(?{}) = {}".format(var,end_value) 
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) >= {} && xsd:dateTime(?{}) < {}".format(var,start_value,var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) >= {}".format(var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= {}".format(var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.EQUAL:
                filter_expr = "xsd:dateTime(?{}) = {}".format(var, start_value)
        else:
            start_var = var[0]
            end_var = var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = {}".format(start_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "xsd:dateTime(?{}) = {}".format(end_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "xsd:dateTime(?{}) = {}".format(end_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "xsd:dateTime(?{}) = {}".format(start_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) >= {} && xsd:dateTime(?{}) < {} || xsd:dateTime(?{}) > {} && xsd:dateTime(?{}) <= {} || xsd:dateTime(?{}) < {} && xsd:dateTime(?{}) > {}".format(start_var,start_value,start_var,end_value,\
                    end_var,start_value,end_var,end_value,\
                        start_var,start_value,end_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) >= {}".format(start_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= {}".format(end_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.EQUAL:
                filter_expr = "xsd:dateTime(?{}) = {}".format(start_var,start_value)
        return "FILTER (" + filter_expr + ")"
    
    def make_temporal_filter_implicit(self,target_var,temporal_relation,related_var):
        if  type(target_var) == type([]) and type(related_var) == type([]):
            target_start_var = target_var[0]
            target_end_var = target_var[1]
            related_start_var = related_var[0]
            related_end_var = related_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_start_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_end_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_end_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_start_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{}) && xsd:dateTime(?{}) < xsd:dateTime(?{}) \
                    || xsd:dateTime(?{}) > xsd:dateTime(?{}) && xsd:dateTime(?{}) <= xsd:dateTime(?{}) \
                    || xsd:dateTime(?{}) < xsd:dateTime(?{}) && xsd:dateTime(?{}) > xsd:dateTime(?{})".format(target_start_var,related_start_var,target_start_var,related_end_var,\
                    target_end_var,related_start_var,target_end_var,related_end_var,\
                        target_start_var,related_start_var,target_end_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{})".format(target_start_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= xsd:dateTime(?{})".format(target_end_var,related_start_var)
        elif type(target_var) == type([]) and type(related_var) == type(""):
            target_start_var = target_var[0]
            target_end_var = target_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_start_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{}) && xsd:dateTime(?{}) < xsd:dateTime(?{})".format(related_var,target_start_var,related_var,target_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{})".format(target_start_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= xsd:dateTime(?{})".format(target_end_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_end_var,related_var)
            else:
                return ""
        elif type(target_var) == type("") and type(related_var) == type([]):
            related_start_var = related_var[0]
            related_end_var = related_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{}) && xsd:dateTime(?{}) < xsd:dateTime(?{})".format(target_var,related_start_var,target_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) >= xsd:dateTime(?{})".format(target_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= xsd:dateTime(?{})".format(target_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_var,related_end_var)
            else:
                return ""
        else:
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "xsd:dateTime(?{}) = xsd:dateTime(?{})".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "xsd:dateTime(?{}) => xsd:dateTime(?{})".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "xsd:dateTime(?{}) <= xsd:dateTime(?{})".format(target_var,related_var)
            else:
                return ""
        return "FILTER (" + filter_expr + ") "

    def make_temporal_order(self,var,rank):
        if rank < 0:
            rank = -rank
            order_expr = "ORDER BY DESC(xsd:dateTime(?{})) ".format(var)
            order_expr += "LIMIT {} OFFSET {}".format(1,rank-1)
        else:
            order_expr = "ORDER BY ASC(xsd:dateTime(?{})) ".format(var)
            order_expr += "LIMIT {} OFFSET {}".format(1,rank-1)
        return order_expr
    
    def make_numeric_cmp_filter(self,var,cmp_sign,number):
        if cmp_sign == "=":
            filter_expr = "str(?{}) = \"{}\"".format(var,number)
        elif cmp_sign == ">=":
            filter_expr = "xsd:double(?{}) >= xsd:double(\"{}\")".format(var,number)
        else:
            filter_expr = "xsd:double(?{}) <= xsd:double(\"{}\")".format(var,number)
        return "FILTER (" + filter_expr + ") "

class QueryGraphWikidataGrounder():
    def __init__(self,graph):
        self.graph = graph
    
    def ground(self):
        sparql = "SELECT DISTINCT "

        #make time var instance
        time_var_map = {}
        time_var_no = 0
        for edge in self.graph.edges:
            if edge.type == Edge.TIME_VALUE_PREDICATE:
                to_node = self.graph.find_node(edge.to_id)
                var = to_node.value
                if type(edge.value) == type(""):
                    time_var_map[var] = "time" + str(time_var_no)
                else:
                    start_var = "start" + str(time_var_no)
                    end_var = "end" + str(time_var_no)
                    time_var_map[var] = [start_var,end_var]
                time_var_no += 1

        #generate select answer vars
        for var in self.graph.answer_vars:
            if var in time_var_map.keys():
                if type(time_var_map[var]) == type(""):
                    sparql += "?" + time_var_map[var] + " "
                else:
                    sparql += "?" + time_var_map[var][0] + " "
                    sparql += "?" + time_var_map[var][1] + " "
            else:
                sparql += "?" + var + " "

        #generate where clause
        sparql += "WHERE{ "
        for edge in self.graph.edges:
            if edge.type == Edge.URI or edge.type == Edge.VAR:
                triple = ""
                from_node = self.graph.find_node(edge.from_id)
                if from_node.type == Node.URI:
                    triple += "<" + from_node.value + ">"
                else:
                    triple += "?" + from_node.value
                triple += " "
                if edge.type == Edge.URI:
                    triple += "<" + edge.value + ">"
                else:
                    triple += "?" + edge.value
                triple += " "
                to_node = self.graph.find_node(edge.to_id)
                if to_node.type == Node.URI:
                    triple += "<" + to_node.value + ">"
                else:
                    triple += "?" + to_node.value
                triple += ". "
                sparql += triple
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                from_node = self.graph.find_node(edge.from_id)
                if from_node.type == Node.URI:
                    from_term = "<" + from_node.value + ">"
                else:
                    from_term = "?" + from_node.value
                to_node = self.graph.find_node(edge.to_id)
                to_node_var = to_node.value
                if type(time_var_map[to_node_var]) == type(""):
                    sparql += from_term + " <" + edge.value + "> " + "?" + time_var_map[to_node_var] + ". "
                else:
                    sparql += from_term + " <" + edge.value[0] + "> " + "?" + time_var_map[to_node_var][0] + ". "
                    sparql += from_term + " <" + edge.value[1] + "> " + "?" + time_var_map[to_node_var][1] + ". "
        
        #generate temporal filter
        for edge in self.graph.edges:
            if edge.type == Edge.TIME_VALUE_RELATION:
                from_node = self.graph.find_node(edge.from_id)
                to_node = self.graph.find_node(edge.to_id)
                if to_node.type == Node.TIME_INTERVAL:
                    filter_expr = self.make_temporal_filter_explicit(time_var_map[from_node.value],edge.value,to_node.value)
                else:
                    filter_expr = self.make_temporal_filter_implicit(time_var_map[from_node.value],edge.value,time_var_map[to_node.value])
                sparql += filter_expr
        
        #generate numerical cmp
        for edge in self.graph.edges:
            if edge.type == Edge.NUMERICAL_CMP:
                var = self.graph.find_node(edge.from_id).value
                cmp_sign = edge.value
                number = self.graph.find_node(edge.to_id).value[0]
                filter_expr = self.make_numeric_cmp_filter(var,cmp_sign,number)
                sparql += filter_expr

        # constraint predicate format
        for edge in self.graph.edges:
            if edge.type == Edge.VAR:
                sparql += "FILTER (regex(str(?" + str(edge.value) + "),\"http://www.wikidata.org/prop/\"))"

        if (self.graph.isentity):
            sparql += "FILTER (regex(str(?" + list(self.graph.answer_vars)[0] + "),\"http://www.wikidata.org/entity/Q\"))"
        #add statement constraint
        #for node in self.graph.nodes:
            #if node.type == Node.VAR and self.graph.check_stmt_var(node.value):
                #sparql += "?{} a <http://wikiba.se/ontology#BestRank>. ".format(node.value)
        sparql += "}"
        
        #generate temporal order
        for edge in self.graph.edges:
            if edge.type == Edge.TEMPORAL_ORDER:
                from_node = self.graph.find_node(edge.from_id)
                rank = self.graph.find_node(edge.to_id).value[0]
                if type(time_var_map[from_node.value]) != type(""):
                    time_var = time_var_map[from_node.value][0]
                else:
                    time_var = time_var_map[from_node.value]
                order_expr = self.make_temporal_order(time_var,rank)
                sparql += order_expr
                break

        return sparql
    
    def make_temporal_filter_explicit(self,var,temporal_relation,time_value):
        start_value = time_value[0]
        end_value = time_value[1]
        if type(var) == type(""):
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = {}".format(var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "?{} = {}".format(var,end_value) 
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} >= {} && ?{} < {}".format(var,start_value,var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= {}".format(var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= {}".format(var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.EQUAL:
                filter_expr = "?{} = {}".format(var,start_value)
        else:
            start_var = var[0]
            end_var = var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = {}".format(start_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "?{} = {}".format(end_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "?{} = {}".format(end_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "?{} = {}".format(start_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} >= {} && ?{} < {} || ?{} > {} && ?{} <= {} || ?{} < {} && ?{} > {}".format(start_var,start_value,start_var,end_value,\
                    end_var,start_value,end_var,end_value,\
                        start_var,start_value,end_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= {}".format(start_var,end_value)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= {}".format(end_var,start_value)
            elif temporal_relation == TIME_VALUE_RELATION.EQUAL:
                filter_expr = "?{} = {}".format(start_var,start_value)
        return "FILTER (" + filter_expr + ")"
    
    def make_temporal_filter_implicit(self,target_var,temporal_relation,related_var):
        if  type(target_var) == type([]) and type(related_var) == type([]):
            target_start_var = target_var[0]
            target_end_var = target_var[1]
            related_start_var = related_var[0]
            related_end_var = related_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = ?{}".format(target_start_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "?{} = ?{}".format(target_end_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "?{} = ?{}".format(target_end_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "?{} = ?{}".format(target_start_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} >= ?{} && ?{} < ?{} || ?{} > ?{} && ?{} <= ?{} || ?{} < ?{} && ?{} > ?{}".format(target_start_var,related_start_var,target_start_var,related_end_var,\
                    target_end_var,related_start_var,target_end_var,related_end_var,\
                        target_start_var,related_start_var,target_end_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= ?{}".format(target_start_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= ?{}".format(target_end_var,related_start_var)
        elif type(target_var) == type([]) and type(related_var) == type(""):
            target_start_var = target_var[0]
            target_end_var = target_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = ?{}".format(target_start_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} >= ?{} && ?{} < ?{}".format(related_var,target_start_var,related_var,target_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= ?{}".format(target_start_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= ?{}".format(target_end_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.MEET:
                filter_expr = "?{} = ?{}".format(target_end_var,related_var)
            else:
                return ""
        elif type(target_var) == type("") and type(related_var) == type([]):
            related_start_var = related_var[0]
            related_end_var = related_var[1]
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = ?{}".format(target_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} >= ?{} && ?{} < ?{}".format(target_var,related_start_var,target_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= ?{}".format(target_var,related_end_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= ?{}".format(target_var,related_start_var)
            elif temporal_relation == TIME_VALUE_RELATION.MET_BY:
                filter_expr = "?{} = ?{}".format(target_var,related_end_var)
            else:
                return ""
        else:
            if temporal_relation == TIME_VALUE_RELATION.BEGIN:
                filter_expr = "?{} = ?{}".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.END:
                filter_expr = "?{} = ?{}".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.OVERLAP:
                filter_expr = "?{} = ?{}".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.AFTER:
                filter_expr = "?{} >= ?{}".format(target_var,related_var)
            elif temporal_relation == TIME_VALUE_RELATION.BEFORE:
                filter_expr = "?{} <= ?{}".format(target_var,related_var)
            else:
                return ""
        return "FILTER (" + filter_expr + ") "

    def make_temporal_order(self,var,rank):
        if rank < 0:
            rank = -rank
            order_expr = "ORDER BY DESC(?{}) ".format(var)
            order_expr += "LIMIT {} OFFSET {}".format(1,rank-1)
        else:
            order_expr = "ORDER BY ASC(?{}) ".format(var)
            order_expr += "LIMIT {} OFFSET {}".format(1,rank-1)
        return order_expr
    
    def make_numeric_cmp_filter(self,var,cmp_sign,number):
        if cmp_sign == "=":
            #filter_expr = "contains(str(?{}),\"{}\")".format(var,number)
            filter_expr = "isLiteral(?{}) && str(?{}) = \"{}\"".format(var,var,number)
        elif cmp_sign == ">=":
            filter_expr = "isLiteral(?{}) && xsd:double(?{}) >= xsd:double(\"{}\")".format(var, var, number)
        else:
            filter_expr = "isLiteral(?{}) && xsd:double(?{}) <= xsd:double(\"{}\")".format(var, var, number)
        return "FILTER (" + filter_expr + ") "

class QueryGraphFreebaseTagger():
    def tag(self,graph):
        var_no = 0
        var_label = {}
        for node in graph.nodes:
            if node.type == Node.VAR or node.type == Node.TIME_VAR:
                if node.value in graph.answer_vars:
                    continue
                var_label[node.value] = "[VAR{}]".format(var_no)
                var_no += 1

        def get_node_label(node):
            if node.type == Node.TIME_INTERVAL:
                return node.value[2]
            elif node.type == Node.LITERAL:
                return str(node.value[0])
            elif node.type == Node.URI:
                label = node.value
                return label.replace("http://rdf.freebase.com/ns/", "")
            elif node.type == Node.VAR or node.type == Node.TIME_VAR:
                if node.value in graph.answer_vars:
                    return "[ANS]"
                else:
                    return var_label[node.value]

        def get_edge_label(edge):
            def get_prop_label(uri):
                localname = uri.split("/")[-1]
                localname = localname.strip().split(".")
                label = ""
                for name in localname:
                    label += " ".join(name.split("_")) + " "
                return label
            if edge.type == Edge.URI:
                return edge.value.replace("http://rdf.freebase.com/ns/", "")
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return edge.value
                else:
                    from_tokens = get_prop_label(edge.value[0]).split()
                    to_tokens = get_prop_label(edge.value[1]).split()
                    prefix_len = 0
                    while prefix_len < min(len(from_tokens),len(to_tokens)) and from_tokens[prefix_len] == to_tokens[prefix_len]:
                        prefix_len += 1
                    label_tokens = from_tokens[:prefix_len] + from_tokens[prefix_len:] + ["and"] + to_tokens[prefix_len:]
                    label = " ".join(label_tokens)
                    return label
            elif edge.type == Edge.TIME_VALUE_RELATION:
                return " ".join(edge.value.name.lower().split("_"))
            elif edge.type == Edge.TEMPORAL_ORDER:
                return "rank"
            elif edge.type == Edge.NUMERICAL_CMP:
                return edge.value
            elif edge.type == Edge.VAR:
                return edge.value

        labels = ""
        
        for edge in graph.edges:
            from_node = graph.find_node(id=edge.from_id)
            to_node = graph.find_node(id=edge.to_id)
            from_label = get_node_label(from_node)
            edge_label = get_edge_label(edge)
            to_label = get_node_label(to_node)
            labels += " " + from_label + " " + edge_label + " " + to_label
        labels = labels
        return labels

class QueryGraphWikidataTagger():
    def tag(self,graph):
        labels = ""
        var_no = 0
        var_label = {}
        for node in graph.nodes:
            if node.type == Node.VAR or node.type == Node.TIME_VAR:
                if node.value in graph.answer_vars:
                    continue
                if node.type == Node.VAR and graph.check_stmt_var(name=node.value):
                    continue
                var_label[node.value] = "[VAR{}]".format(var_no)
                var_no += 1
        
        def get_node_label(node_id):
            node = graph.find_node(node_id)
            if node.type == Node.TIME_VAR or node.type == Node.VAR:
                if node.value in graph.answer_vars:
                    return "[ANS]"
                elif(node.value in var_label):
                    return var_label[node.value]
                else:
                    return str(node.value)
            elif node.type == Node.URI:
                if (node.value == "http://wikiba.se/ontology#Statement"):
                    return "statement"
                else:
                    return node.value.replace("http://www.wikidata.org/entity/","")
            elif node.type == Node.TIME_INTERVAL:
                return node.value[0].split('"')[1]
            else:
                return str(node.value[0])
        
        def get_edge_label(edge):
            if edge.type == Edge.URI:
                if (edge.value == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"):
                    return "type"
                return edge.value.replace("http://www.wikidata.org/prop/","")
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return wd.query_label(edge.value)
                else:
                    return wd.query_pair_label(*edge.value)
            elif edge.type == Edge.TIME_VALUE_RELATION:
                return " ".join(edge.value.name.lower().split("_"))
            elif edge.type == Edge.TEMPORAL_ORDER:
                return "rank"
            elif edge.type == Edge.NUMERICAL_CMP:
                return edge.value
            elif edge.type == Edge.VAR:
                return edge.value

        for var in graph.vars:
            if graph.check_stmt_var(name=var):
                in_edges = graph.get_node_in_edges(name=var)
                out_edges = graph.get_node_out_edges(name=var)
                subject_label = ""
                predicate_label = ""
                object_label = ""
                constraints = []
                if len(in_edges) > 0:
                    in_edge = in_edges[0]
                    subject_label = get_node_label(in_edge.from_id)
                    predicate_label = wd.query_label(in_edge.value)
                for out_edge in out_edges:
                    edge_label = get_edge_label(out_edge)
                    to_label = get_node_label(out_edge.to_id)
                    if out_edge.type == Edge.URI and check_prefix(out_edge.value,"ps"):
                        predicate_label = edge_label
                        object_label = to_label
                    elif out_edge.type == Edge.TIME_VALUE_PREDICATE and type(out_edge.value) == type("") and check_prefix(out_edge.value,"ps"):
                        predicate_label = edge_label
                        object_label = to_label
                    else:
                        constraints.append(edge_label + " " + to_label)
                stmt = subject_label + " " + predicate_label + " " + object_label
                if len(constraints) > 0:
                    stmt += " and " + " and ".join(constraints)
                labels += stmt + " "
        
        for edge in graph.edges:
            from_node = graph.find_node(id=edge.from_id)
            to_node = graph.find_node(id=edge.to_id)
            if from_node.type == Node.VAR and graph.check_stmt_var(name=from_node.value):
                continue
            if to_node.type == Node.VAR and graph.check_stmt_var(name=to_node.value):
                continue
            from_label = get_node_label(edge.from_id)
            edge_label = get_edge_label(edge)
            to_label = get_node_label(edge.to_id)
            labels += " " + from_label + " " + edge_label + " " + to_label
        labels = " ".join(labels.split())
        if (graph.isentity):
            labels += "answer entity con"
        return labels