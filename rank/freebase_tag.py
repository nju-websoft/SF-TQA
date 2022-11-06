from query.querygraph import Node,Edge
from utils.freebase_utils import query_label

class FreebaseTagger():
    def tag(self,graph):
        entity_linking = graph.parsed_question.entity_linking
        ent2mention = {link.uri:mention for mention,link in entity_linking.items()}

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
                return node.value[2].replace("?","")
            elif node.type == Node.LITERAL:
                return str(node.value[0])
            elif node.type == Node.URI:
                if node.value in ent2mention:
                    return ent2mention[node.value]
                label = query_label(node.value)
                return label
            elif node.type == Node.VAR or node.type == Node.TIME_VAR:
                if node.value in graph.answer_vars:
                    return "[ANS]"
                else:
                    return var_label[node.value]

        def get_edge_label(edge):
            def get_prop_label(uri):
                localname = uri.split("/")[-1]
                #localname = localname.strip().split(".")
                localname = localname.strip().split(".")[-1:]
                label = ""
                for name in localname:
                    label += " ".join(name.split("_")) + " "
                return label
            if edge.type == Edge.URI:
                if edge.value == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>":
                    return "is a"
                return get_prop_label(edge.value)
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return get_prop_label(edge.value)
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
        labels = " ".join(labels.split())
        return labels

class FreebasePathRankTagger():
    def tag(self,graph):
        entity_linking = graph.parsed_question.entity_linking
        ent2mention = {link.uri:mention for mention,link in entity_linking.items()}

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
                if node.value in ent2mention:
                    return ent2mention[node.value]
                label = query_label(node.value)
                return label
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
                return get_prop_label(edge.value)
            elif edge.type == Edge.TIME_VALUE_PREDICATE:
                if type(edge.value) == type(""):
                    return get_prop_label(edge.value)
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