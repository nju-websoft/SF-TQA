from SPARQLWrapper import SPARQLWrapper,JSON
from utils.prefix_utils import check_prefix, parse_id, parse_prefix,get_uri
from utils.dataset_utils import DATASET
import json
import pyodbc

dataset_type = ""
wikidata_url = "http://114.212.81.217:8890/sparql/"
crq_wikidata_graph_uri = "http://wikidata.org/crq"
endpoint = None
odbc_wikidata = None
time_predicate_pairs = set()
time_predicates = set()
temporal_predicates = set()
temporal_relation_predicates = set()
part_of_predicates = set()
label_dict = {}
time_event = set()
res_cache = {}
res_cache_add = {}
onehop_relation_cache = {}
relation_for_event_cache = {}
two_hop_path_with_event_cache = {}
ord_seq_list = {}

def wikidata_init(dt):
    global dataset_type
    global endpoint
    global time_predicate_pairs
    global time_predicates
    global temporal_predicates
    global part_of_predicates
    global label_dict
    global res_cache
    global res_cache_add
    global onehop_relation_cache
    global relation_for_event_cache
    global two_hop_path_with_event_cache
    global odbc_wikidata

    # with open("data/wikidata/query_cache.json","rt",encoding="utf-8") as fin:
    #     for line in fin:
    #         res = json.loads(line.strip())
    #         res_cache[res["label"]] = res["res"]

    with open("data/wikidata/onehop_relation_cache.json","rt",encoding="utf-8") as fin:
        data = json.load(fin)
        for key,value in data.items():
            tmp = {}
            tmp["in_paths"] = value[1]
            tmp["out_paths"] = value[0]
            onehop_relation_cache[key] = tmp

    with open("data/wikidata/two_hop_path_with_event_cache.json","rt",encoding="utf-8") as fin:
        two_hop_path_with_event_cache = json.load(fin)

    with open("data/wikidata/relation_for_event_cache.json","rt",encoding="utf-8") as fin:
        relation_for_event_cache = json.load(fin)

    with open("data/wikidata/label_dict.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            line.strip()
            wid = line.split('\t')[0]
            label = '\t'.join(line.split('\t')[1:])
            label_dict[wid] = label

    with open("data/wikidata/part_of_predicates.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            uri = line.strip()
            part_of_predicates.add(uri)

    with open("data/wikidata/event_dict.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            uri = line.strip()
            time_event.add(uri)
    
    with open("data/wikidata/TimeProps.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            uri = line.strip()
            pid = parse_id(uri)
            time_predicates.add(pid)

    with open("data/wikidata/temporal_relation.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            uri = line.strip()
            temporal_relation_predicates.add(uri)
    
    with open("data/wikidata/TimePropsPair.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            start,end = line.strip().split()
            start = parse_id(start)
            end = parse_id(end)
            time_predicate_pairs.add((start,end))
    
    with open("data/wikidata/pid_to_temporal_entity.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            pid = line.strip()
            temporal_predicates.add(pid)
    
    with open("data/wikidata/label_dict.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            line.strip()
            wid = line.split('\t')[0]
            label = '\t'.join(line.split('\t')[1:])
            label_dict[wid]=label
    
    dataset_type = dt
    if dataset_type == DATASET.CRQ:
        endpoint = SPARQLWrapper(wikidata_url)
        endpoint.setReturnFormat(JSON)
    elif dataset_type == DATASET.TQ:
        endpoint = SPARQLWrapper(wikidata_url)
        endpoint.setReturnFormat(JSON)

def odbc_init():
    global odbc_wikidata
    odbc_wikidata = pyodbc.connect('DSN=Virtuoso;UID=dba;PWD=dba')
    odbc_wikidata.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    odbc_wikidata.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    odbc_wikidata.setencoding(encoding='utf-8')

def write_cache():
    with open("data/wikidata/query_cache.json", "a+", encoding="utf-8") as fin:
        for key,value in res_cache_add.items():
            res = {}
            res["label"] = key
            res["res"] = value
            fin.write(json.dumps(res)+"\n")

def query_wikidata_with_odbc(sparql,limit=-1):
    if not "limit" in sparql and not "LIMIT" in sparql:
        if limit == -1:
            sparql += " limit 300"
        elif limit >= 0:
            sparql += " limit {}".format(limit)
    global odbc_wikidata
    if odbc_wikidata == None:
        odbc_init()
    query = "SPARQL "+sparql
    try:
        with odbc_wikidata.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    except Exception as err:
        print(err)
        print(sparql)
        odbc_init()
        return []
    results = parse_query_results_by_odbc(rows)
    return list(results)

def wikidata_SPARQLWrapper_init():
    global endpoint
    endpoint = SPARQLWrapper(wikidata_url)
    endpoint.setReturnFormat(JSON)

def query_wikidata(sparql,limit=-1):
    if not "limit" in sparql and not "LIMIT" in sparql:
        if limit == -1:
            sparql += " limit 300"
        elif limit >= 0:
            sparql += " limit {}".format(limit)
    global endpoint
    try:
        endpoint.setQuery(sparql)
        endpoint.setReturnFormat(JSON)
    except:
        wikidata_SPARQLWrapper_init()
        endpoint.setQuery(sparql)
        endpoint.setReturnFormat(JSON)
    try:
        response = endpoint.query().convert()
    except Exception as err:
        print(err)
        wikidata_SPARQLWrapper_init()
        return []
    results = parse_query_results(response,sparql)
    return results

def parse_query_results_by_odbc(response):
    if(len(response)==0):
        return []
    result = []
    var_num = len(response[0])
    for res in response:
        key = res.cursor_description
        res = {key[i][0] : res[i].replace(" 00:00:00Z", "T00:00:00Z") for i in range(var_num) if(res[i])}
        result.append(res)
    return result

def parse_query_results(response, query):
    if 'ASK' in query or 'ask' in query:  # ask
        result = response['boolean']
    elif 'COUNT' in query or 'count' in query:  # count
        result = int(response['results']['bindings'][0]['callret-0']['value'])
    else:
        result = []
        for res in response['results']['bindings']:
            res = {k: v["value"] for k, v in res.items()}
            result.append(res)
    return result

def is_time_pair_predicate(start_uri,end_uri):
    if start_uri.startswith("http://www.wikidata.org/prop/qualifier/value/") or end_uri.startswith("http://www.wikidata.org/prop/qualifier/value/"):
        return False
    elif start_uri.startswith("http://www.wikidata.org/prop/") and end_uri.startswith("http://www.wikidata.org/prop/"):
        start_id = parse_id(start_uri)
        start_prefix = parse_prefix(start_uri)
        end_id = parse_id(end_uri)
        end_prefix = parse_prefix(end_uri)
        if (start_id,end_id) in time_predicate_pairs and start_prefix == end_prefix:
            return True
        else:
            return False
    elif start_uri.startswith("P") and end_uri.startswith("P"):
        if (start_uri,end_uri) in time_predicate_pairs:
            return True
        else:
            return False
    return False

def is_time_predicate(uri):
    if uri.startswith("http://www.wikidata.org/prop/qualifier/value/"):
        return False
    elif uri.startswith("http://www.wikidata.org/prop/"):
        pid = parse_id(uri)
        if pid in time_predicates and (check_prefix(uri,"pq") or check_prefix(uri,"wdt") or check_prefix(uri,"ps")):
            return True
        else:
            return False
    elif uri.startswith("P") and uri in temporal_predicates:
        return True
    else:
        return False

def is_temporal_predicate(uri):
    if uri.startswith("http://www.wikidata.org/prop/"):
        pid = parse_id(uri)
        if pid in temporal_predicates and (check_prefix(uri,"pq") or check_prefix(uri,"wdt") or check_prefix(uri,"ps")):
            return True
        else:
            return False
    else:
        if uri in temporal_predicates:
            return True
        else:
            return False

def is_start(uri):
    uri = parse_id(uri)
    for start,_ in time_predicate_pairs:
        if uri == start:
            return True
    return False

def is_end(uri):
    uri = parse_id(uri)
    for _,end in time_predicate_pairs:
        if uri == end:
            return True
    return False

def get_start_predicate(uri):
    uri = parse_id(uri)
    for start,end in time_predicate_pairs:
        if uri == end:
            return start
    return None

def get_end_predicate(uri):
    uri = parse_id(uri)
    for start,end in time_predicate_pairs:
        if uri == start:
            return end
    return None

def query_out_paths_for_event_statement(uri):
    if (parse_id(uri) in relation_for_event_cache):
        return relation_for_event_cache[parse_id(uri)]["out_paths"], relation_for_event_cache[parse_id(uri)][
            "has_out_paths_statement_constraint"]
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p1 ?p2 ?p WHERE {{
        {{
          <{}> ?p1 ?s.
          ?s ?p2 ?o.
          optional{{ ?s ?p <http://wikiba.se/ontology#Statement> }}
          FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
          FILTER (!strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
          FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))         
          FILTER EXISTS {{ ?s ?q ?t. FILTER (datatype(?t)=xsd:dateTime)}}
        }}
        UNION
        {{
          <{}> ?p1 ?s.
          ?s ?p2 ?o.
          optional{{ ?s ?p <http://wikiba.se/ontology#Statement> }}
          FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
          FILTER (!strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
          FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))         
          FILTER (strstarts(str(?o),"http://www.wikidata.org/entity"))
          FILTER EXISTS {{?o ?r ?q. FILTER (datatype(?q)=xsd:dateTime)}}
        }}
        }}""".format(uri, uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    is_statement_type = False
    for result in results:
        if(len(result)==3):
            is_statement_type = True
            break
    for result in results:
        if((is_statement_type and len(result)==3) or not is_statement_type):
            p1 = result["p1"]
            p2 = result["p2"]
            props.append([p1, p2])
    return props,is_statement_type

def query_in_paths_for_event_statement(uri):
    if (parse_id(uri) in relation_for_event_cache):
        return relation_for_event_cache[parse_id(uri)]["in_paths"], relation_for_event_cache[parse_id(uri)][
            "has_in_paths_statement_constraint"]
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p1 ?p2 ?p WHERE {{
        {{
          ?x ?p2 ?s.
          ?s ?p1 <{}>.
          optional{{ ?s ?p <http://wikiba.se/ontology#Statement> }}
          FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
          FILTER (!strstarts(str(?p2),"http://www.wikidata.org/prop/direct/P"))
          FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p1),"http://www.wikidata.org/prop/qualifier/P"))         
          FILTER EXISTS {{ ?s ?q ?t. FILTER (datatype(?t)=xsd:dateTime)}}
        }}
        UNION
        {{
          ?x ?p2 ?s.
          ?s ?p1 <{}>.
          optional{{ ?s ?p <http://wikiba.se/ontology#Statement> }}
          FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
          FILTER (!strstarts(str(?p2),"http://www.wikidata.org/prop/direct/P"))
          FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p1),"http://www.wikidata.org/prop/qualifier/P"))         
          FILTER (strstarts(str(?x),"http://www.wikidata.org/entity/Q"))
          FILTER EXISTS {{?x ?r ?q. FILTER (datatype(?q)=xsd:dateTime)}}
        }}
        }}
        """.format(uri,uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    is_statement_type = False
    for result in results:
        if (len(result) == 3):
            is_statement_type = True
            break
    for result in results:
        if ((is_statement_type and len(result) == 3) or not is_statement_type):
            p1 = result["p1"]
            p2 = result["p2"]
            props.append([p1, p2])
    return props,is_statement_type

def query_onehop_out_paths(uri):
    if (uri in onehop_relation_cache):
        return onehop_relation_cache[uri]["out_paths"]
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p1 ?p2 WHERE {{
        <{}> ?p1 ?s.
        ?s ?p2 ?o.
        FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
        FILTER (!strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
        FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
        }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1,p2])
    return props

def query_onehop_in_paths(uri):
    if(uri in onehop_relation_cache):
        return onehop_relation_cache[uri]["in_paths"]
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p1 ?p2 WHERE {{
        ?x ?p2 ?s.
        ?s ?p1 <{}>.
        FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/statement/"))
        FILTER (!strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
        FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p1),"http://www.wikidata.org/prop/qualifier/P"))
        }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1,p2])
    return props

def is_part_of_for_statement(entity1, entity2):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p ?s.
            ?s <http://www.wikidata.org/prop/qualifier/P805> <{}>.
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/P"))
            }}""".format(entity1,entity2)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p = result["p"]
        props.append(p)
    return props

def query_temporal_onehop_out_paths(uri):
    props = query_onehop_out_paths(uri)
    time_props = []
    for prop in props:
        if is_time_predicate(prop[1]):
            time_props.append(prop)
    return time_props

def query_stmts(subject,object):
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p1 ?p2 WHERE {{
        <{}> ?p1 ?s.
        ?s ?p2 <{}>.
        FILTER (regex(str(?p1),"http://www.wikidata.org/prop/P"))
        FILTER (regex(str(?p2),"http://www.wikidata.org/prop/statement/P"))
        }}""".format(subject,object)
    results = query_wikidata_with_odbc(sparql)
    preds = []
    for result in results:
        p1 = result["p1"]
        pid = parse_id(p1)
        preds.append(pid)
    return preds

def query_stmt_time_qualifiers(pred,subject,object):
    p_pred = get_uri("p:" + pred)
    ps_pred = get_uri("ps:" + pred)
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p WHERE {{
        <{}> <{}> ?stmt.
        ?stmt <{}> <{}>.
        ?stmt ?p ?o.
        FILTER (regex(str(?p),"http://www.wikidata.org/prop/qualifier/P"))
        }}""".format(subject,p_pred,ps_pred,object)
    results = query_wikidata_with_odbc(sparql)
    time_preds = []
    for result in results:
        if is_time_predicate(result["p"]):
            time_preds.append(result["p"])
    return time_preds

def query_label(uri):
    if uri.startswith("P") or uri.startswith("Q"):
        if uri in label_dict.keys():
            return label_dict[uri]
        else:
            uri = get_uri("wd:"+uri)
    if uri.startswith("http://www.wikidata.org/"):
        wid = parse_id(uri)
        if wid in label_dict.keys():
            return label_dict[wid]
    if(uri=='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'):
        return "type"
    id = parse_id(uri)
    uri = "http://www.wikidata.org/entity/" + id
    sparql = "SELECT ?label WHERE {{ <{}> rdfs:label ?label.FILTER(langMatches(lang(?label),\"en\"))}}".format(uri)
    results = query_wikidata_with_odbc(sparql)
    if len(results) > 0:
        return results[0]["label"]
    else:
        return ""

def query_pair_label(start,end):
    start_id = parse_id(start)
    end_id = parse_id(end)
    start_label = query_label(start_id)
    end_label = query_label(end_id)
    return start_label + " and " + end_label

def get_statement_path_to_time(qid):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p1 ?p2 WHERE {{
            <{}> ?p1 ?s0.
            ?s0 ?p2 ?s.
            FILTER (datatype(?s)=xsd:dateTime)
            FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
            }}""".format(qid)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1, p2])
    return props

def get_statement_path_to_value(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p1 ?p2 WHERE {{
            <{}> ?p1 ?s0.
            ?s0 ?p2 ?s.
            FILTER (datatype(?s)=xsd:decimal)
            FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P"))
            }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1, p2])
    return props

def get_time_direct_entity(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?e WHERE {{
            <{}> ?p ?e.
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (datatype(?e)=xsd:dateTime)
            }}""".format(uri)
    results = query_wikidata(sparql)
    props = []
    for result in results:
        p = result["e"]
        props.append(p)
    return props

def get_time_and_predicate_around_entity(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?s WHERE {{
            <{}> ?p ?e.
            ?e ?q ?s.
            FILTER (!strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (!strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (datatype(?s)=xsd:dateTime)
            }}""".format(uri)
    results = query_wikidata(sparql)
    props = []
    for result in results:
        p = result["s"]
        props.append(p)
    return props

def get_twohop_path_to_time(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            <{}> ?p ?e.
            ?e ?q ?s.
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (datatype(?s)=xsd:dateTime)
            }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def get_onehop_path_to_time(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p ?s.
            FILTER (datatype(?s)=xsd:dateTime)
            }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        props.append(p1)
    return props

def get_onehop_path_to_time_event(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p ?s.
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
            }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        props.append(p1)
    return props

def get_onehop_path_from_time_event(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            ?s ?p <{}>.
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}} )
            }}""".format(uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        props.append(p1)
    return props

def get_twohop_path_with_time_event(uri,var):
    if(parse_id(uri) in two_hop_path_with_event_cache):
        if(var=="s0"):
            return two_hop_path_with_event_cache[parse_id(uri)]
        else:
            res = []
            for tuple in two_hop_path_with_event_cache[parse_id(uri)]:
                tmp = [i.replace("s0",var) for i in tuple]
                res.append(tmp)
            return res
    res = []
    props = get_twohop_path_from_entity_from_time_event_to_ans(uri)
    for result in props:
        p1 = result[0]
        p2 = result[1]
        res.append([uri, p1, var, var, p2, var+"Aspect"])
    props = get_twohop_path_from_entity_from_ans_to_time_event(uri)
    for result in props:
        p1 = result[0]
        p2 = result[1]
        res.append([uri, p1, var, var+"Aspect", p2, var])
    props = get_twohop_path_to_entity_from_ans_to_time_event(uri)
    for result in props:
        p1 = result[0]
        p2 = result[1]
        res.append([var+"Aspect", p2, var, var, p1, uri])
    props = get_twohop_path_to_entity_from_time_event_to_ans(uri)
    for result in props:
        p1 = result[0]
        p2 = result[1]
        res.append([var, p2, var+"Aspect", var, p1, uri])
    return res

def get_twohop_path_from_entity_from_time_event_to_ans(uri):# time event--> ans
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            <{}> ?p ?s.
            ?s ?q ?e.
            FILTER (<{}> != ?e)
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/Q"))
            FILTER (strstarts(str(?e),"http://www.wikidata.org/entity/Q"))
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
            }}""".format(uri,uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def get_twohop_path_from_entity_from_ans_to_time_event(uri):# time event<--ans
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            <{}> ?p ?s.
            ?e ?q ?s.
            FILTER (<{}> != ?e)
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/Q"))
            FILTER (strstarts(str(?e),"http://www.wikidata.org/entity/Q"))
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
            }}""".format(uri,uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def get_twohop_path_to_entity_from_ans_to_time_event(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            ?e ?q ?s.
            ?s ?p <{}>.
            FILTER (<{}> != ?e)
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/Q"))
            FILTER (strstarts(str(?e),"http://www.wikidata.org/entity/Q"))
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
            }}""".format(uri,uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def get_twohop_path_to_entity_from_time_event_to_ans(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            ?s ?q ?e.
            ?s ?p <{}>.
            FILTER (<{}> != ?e)
            FILTER (strstarts(str(?p),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?q),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?s),"http://www.wikidata.org/entity/Q"))
            FILTER (strstarts(str(?e),"http://www.wikidata.org/entity/Q"))
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
            }}""".format(uri,uri)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def is_part_of(qid1,qid2):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p1 ?p2 WHERE {{
            <{}> ?p1 ?s.
            ?s ?p2 <{}>.
            FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
            }}""".format(qid1, qid2)
    results = query_wikidata_with_odbc(sparql)
    paths = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        if (parse_id(p1) in part_of_predicates and parse_id(p2) in part_of_predicates):
            paths.append([p1, p2])
    return paths

def get_path_between_two_entity(qid1,qid2):
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p1 ?p2 WHERE {{
        {{
            <{}> ?p1 ?s.
            ?s ?p2 <{}>.
            FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
        }}
        UNION
        {{
            <{}> ?p1 ?s.
            ?s ?p2 <{}>.
            FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
            FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/direct/P"))
            FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
        }}
        }}""".format(qid1,qid2,qid1,qid2)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1,p2])
    return props

def get_path_to_two_entity(qid1,qid2):
    sparql = """
            PREFIX wikibase: <http://wikiba.se/ontology#>
            SELECT DISTINCT ?p1 ?p2 WHERE {{
                {{
                    ?s ?p1 <{}>.
                    ?s ?p2 <{}>.
                    FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
                }}
                UNION
                {{
                    ?s ?p1 <{}>.
                    ?s ?p2 <{}>.
                    FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
                    FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/direct/P"))
                    FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
                    }}
                }}""".format(qid1, qid2, qid1, qid2)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1, p2])
    return props

def get_path_from_two_entity(qid1,qid2):
    sparql = """
            PREFIX wikibase: <http://wikiba.se/ontology#>
            SELECT DISTINCT ?p1 ?p2 WHERE {{
                {{
                    <{}> ?p1 ?s.
                    <{}> ?p2 ?s.
                    FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/statement/P")||strstarts(str(?p2),"http://www.wikidata.org/prop/qualifier/P"))
                }}
                UNION
                {{
                    <{}> ?p1 ?s.
                    <{}> ?p2 ?s .
                    FILTER (strstarts(str(?p1),"http://www.wikidata.org/prop/direct/P"))
                    FILTER (strstarts(str(?p2),"http://www.wikidata.org/prop/direct/P"))
                    FILTER(EXISTS{{?c wdt:P793 ?s.}} OR EXISTS{{ ?s p:P793 ?a.?a ps:P793 wd:Q1190554.}})
                    }}
                }}""".format(qid1, qid2, qid1, qid2)
    results = query_wikidata_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p1"]
        p2 = result["p2"]
        props.append([p1, p2])
    return props


def is_statement_node(uri):
    return get_uri(uri).startswith("http://www.wikidata.org/entity/statement")

if __name__ == "__main__":
    # sparql = """
    # SELECT ?ans ?s WHERE {wd:Q242437 p:P166 ?s.?s pq:P580 ?ans.?ans ?s wd:Q242437}
    # """
    # results = query_wikidata(sparql)
    # print(odbc_init("SPARQL "+sparql))
    wikidata_init(DATASET.TQ)
    print(query_in_paths_for_event_statement("http://www.wikidata.org/entity/Q29"))
    # print(results)

    # el = []
    # with open("data/wikidata/entity_in_TQ.txt", "r", encoding="utf-8") as f:
    #     for line in f.readlines():
    #         el.append(line.strip())
    # onehop_relation_cache = {}
    # for i in el:
    #     out_path = query_onehop_out_paths(i)
    #     in_path = query_onehop_in_paths(i)
    #     onehop_relation_cache[i] = [out_path,in_path]
    # with open("data/wikidata/onehop_relation_cache.json","w+",encoding="utf-8") as f:
    #     json.dump(onehop_relation_cache,fp=f,indent=2)

    # el = []
    # with open("data/wikidata/entity_in_TQ.txt", "r", encoding="utf-8") as f:
    #     for line in f.readlines():
    #         el.append(line.strip())
    # onehop_relation_for_event_cache = {}
    # for i in el:
    #     out_path, statement1 = query_out_paths_for_event(i)
    #     in_path, statement2 = query_in_paths_for_event(i)
    #     tmp = {}
    #     tmp["out_paths"] = out_path
    #     tmp["has_out_paths_statement_constraint"] = statement1
    #     tmp["in_paths"] = in_path
    #     tmp["has_in_paths_statement_constraint"] = statement2
    #     onehop_relation_for_event_cache[i] = tmp
    # with open("data/wikidata/relation_for_event_cache.json", "w+", encoding="utf-8") as f:
    #     json.dump(onehop_relation_for_event_cache, fp=f)


