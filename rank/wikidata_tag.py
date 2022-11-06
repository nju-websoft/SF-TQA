from query.querygraph import Node,Edge
from utils.wikidata_utils import query_label,query_pair_label,check_prefix

class WikidataTagger():
    def tag(self,graph):
        entity_linking = graph.parsed_question.entity_linking
        ent2mention = {link.uri:mention for mention,link in entity_linking.items()}
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
                if node.value in ent2mention.keys():
                    return ent2mention[node.value]
                else:
                    if(node.value=="http://wikiba.se/ontology#Statement"):
                        return "statement"
                    return query_label(node.value)
            elif node.type == Node.TIME_INTERVAL:
                return node.value[0].split('"')[1]
            else:
                return str(node.value[0])
        
        def get_edge_label(edge):
            if edge.type == Edge.URI:
                if(edge.value=="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"):
                    return "type"
                return query_label(edge.value)
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return query_label(edge.value)
                else:
                    return query_pair_label(*edge.value)
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
                    predicate_label = query_label(in_edge.value)
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
                        if to_label == "statement":
                            continue
                        constraints.append(edge_label + " " + to_label)
                stmt = subject_label + " " + predicate_label + " " + object_label
                if len(constraints) > 0:
                    stmt += " with " + " with ".join(constraints)
                labels += stmt + " . "
        
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
        return labels

class WikidataPathRankTagger():
    def tag(self,graph):
        entity_linking = graph.parsed_question.entity_linking
        ent2mention = {link.uri:mention for mention,link in entity_linking.items()}
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
                if node.value in ent2mention.keys():
                    return ent2mention[node.value]
                else:
                    if(node.value=="http://wikiba.se/ontology#Statement"):
                        return "statement"
                    return query_label(node.value)
            elif node.type == Node.TIME_INTERVAL:
                return node.value[0].split('"')[1]
            else:
                return str(node.value[0])
        
        def get_edge_label(edge):
            if edge.type == Edge.URI:
                return query_label(edge.value)
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return query_label(edge.value)
                else:
                    return query_pair_label(*edge.value)
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
                    predicate_label = query_label(in_edge.value)
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
                        if to_label == "statement":
                            continue
                        constraints.append(edge_label + " " + to_label)
                stmt = subject_label + " " + predicate_label + " " + object_label
                if len(constraints) > 0:
                    stmt += " with " + " with ".join(constraints)
                labels += stmt + " . "
        
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
        return labels