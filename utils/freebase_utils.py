from SPARQLWrapper import SPARQLWrapper,JSON
from utils.dataset_utils import DATASET
import json
import pyodbc

dataset_type = ""
freebase_url = "http://210.28.134.34:8890/sparql/"
endpoint = None
time_predicate_pairs = set()
time_predicates = set()
part_of_predicates = set()
label_dict = {}
prefix2uri = {}
uri2prefix = {}
res_cache = {}
res_cache_add = {}
onehop_relation_cache = {}
one_or_two_hop_paths_cache = {}
twohop_path_to_time_cache = {}
one_or_twohop_paths_for_event_cache = {}
odbc_conn = None

def freebase_init(dt):
    global dataset_type
    global endpoint
    global time_predicate_pairs
    global time_predicates
    global part_of_predicates
    global prefix2uri
    global uri2prefix
    global res_cache
    global res_cache_add
    global onehop_relation_cache
    global one_or_two_hop_paths_cache
    global twohop_path_to_time_cache
    global one_or_twohop_paths_for_event_cache
    global odbc_conn

    # with open("data/freebase/query_cache.json","rt",encoding="utf-8") as fin:
    #     for line in fin:
    #         res = json.loads(line.strip())
    #         res_cache[res["label"]] = res["res"]

    with open("data/freebase/one_or_two_hop_paths_for_event_cache.json","rt",encoding="utf-8") as fin:
        data = json.load(fin)
        for key,value in data.items():
            one_or_twohop_paths_for_event_cache[key] = value

    with open("data/freebase/onehop_relation_cache.json", "rt", encoding="utf-8") as fin:
        data = json.load(fin)
    for key, value in data.items():
        tmp = {}
        tmp["in_paths"] = value[1]
        tmp["out_paths"] = value[0]
        onehop_relation_cache[key] = tmp

    with open("data/freebase/one_or_two_hop_paths_cache.json","rt",encoding="utf-8") as f:
        one_or_two_hop_paths_cache = json.load(f)

    with open("data/freebase/twohop_path_to_time_cache.json","rt",encoding="utf-8") as f:
        twohop_path_to_time_cache = json.load(f)

    with open("data/freebase/label_dict.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            line.strip()
            mid = line.split('\t')[0]
            label = '\t'.join(line.split('\t')[1:])
            label_dict[mid] = label

    with open("data/freebase/part_of_predicates.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            part_of_predicates.add(line.strip())
    
    with open("data/freebase/time_predicates.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            time_predicates.add(line.strip())
    
    with open("data/freebase/time_predicate_pairs.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            start,end = line.strip().split()
            time_predicates.add(start)
            time_predicates.add(end)
            time_predicate_pairs.add((start,end))
    
    with open("data/freebase/prefix.txt","rt",encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            prefix = line.split("\t")[0]
            uri = line.split("\t")[1]
            prefix2uri[prefix] = uri
            uri2prefix[uri] = prefix
    
    endpoint = SPARQLWrapper(freebase_url,defaultGraph="http://freebase.com")
    dataset_type = dt

def write_freebase_cache():
    with open("data/freebase/query_cache.json", "a+", encoding="utf-8") as fin:
        for key,value in res_cache_add.items():
            res = {}
            res["label"] = key
            res["res"] = value
            fin.write(json.dumps(res)+"\n")

def freebase_SPARQLWrapper_init():
    global endpoint
    endpoint = SPARQLWrapper(freebase_url,defaultGraph="http://freebase.com")

def freebase_init_odbc():
    global odbc_conn
    odbc_conn = pyodbc.connect(r'DRIVER=/home2/xxhu/virtuoso-opensource/lib/virtodbc.so'
                               r';HOST=210.28.134.34:1111'
                               r';UID=dba'
                               r';PWD=dba'
                               )
    odbc_conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf8')
    odbc_conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf8')
    odbc_conn.setencoding(encoding='utf8')
    print('Freebase Virtuoso ODBC connected')

def parse_query_results_by_odbc(response):
    if(len(response)==0):
        return []
    result = []
    var_num = len(response[0])
    for res in response:
        key = res.cursor_description
        res = {key[i][0] : res[i] for i in range(var_num) if(res[i])}
        result.append(res)
    return result

def query_freebase_with_odbc(sparql,limit=-1):
    if not "limit" in sparql and not "LIMIT" in sparql:
        if limit == -1:
            sparql += " limit 300"
        elif limit >= 0:
            sparql += " limit {}".format(limit)
    global odbc_conn
    if odbc_conn == None:
        freebase_init_odbc()

    query2 = "SPARQL " + sparql
    try:
        with odbc_conn.cursor() as cursor:
            cursor.execute(query2)
            rows = cursor.fetchall()
    except Exception as err:
        print(err)
        print(sparql)
        freebase_init_odbc()
        return []

    results = parse_query_results_by_odbc(rows)

    return list(results)


def query_freebase(sparql,limit=-1):
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
        freebase_SPARQLWrapper_init()
        endpoint.setQuery(sparql)
        endpoint.setReturnFormat(JSON)
    try:
        response = endpoint.query().convert()
    except Exception as err:
        #print(err)
        freebase_SPARQLWrapper_init()
        return []
    results = parse_query_results(response,sparql)
    return results

def parse_query_results(response, query):
    if query.startswith("ASK") or query.startswith('ask'):  # ask
        result = response['boolean']
    elif 'COUNT(' in query or 'count(' in query:  # count
        result = int(response['results']['bindings'][0]['callret-0']['value'])
    else:
        result = []
        for res in response['results']['bindings']:
            res = {k: v["value"] for k, v in res.items()}
            result.append(res)
    return result

def get_uri_by_key(key):
    if key.startswith("http"):
        return key
    if key.startswith("en."):
        key = key.replace("en.","/en/")
    sparql = """SELECT DISTINCT ?x WHERE {{
        ?x ns:type.object.key \"{}\".
    }}""".format(key)
    results = query_freebase_with_odbc(sparql)
    uris = list(map(lambda x:x["x"],results))
    if len(uris) > 0:
        return uris[0]
    else:
        return None

def parse_uri(uri):
    for key in uri2prefix.keys():
        if(uri.startswith(key)):
            return uri[len(key):]
    return uri

def get_uri(prefix,localname):
    prefix_uri = prefix2uri[prefix]
    return prefix_uri + localname

def check_prefix(prefix,uri):
    localname = uri.split("/")[-1]
    prefix_uri = uri.split(localname)[0]
    if prefix in prefix2uri and prefix2uri[prefix] == prefix_uri:
        return True
    return False

def is_time_pair_predicate(start_uri,end_uri):
    for cur_start_uri,cur_end_uri in time_predicate_pairs:
        if cur_start_uri == start_uri and cur_end_uri == end_uri:
            return True
    return False

def is_time_predicate(uri):
    if uri in time_predicates:
        return True
    return False

def is_start(uri):
    for start,_ in time_predicate_pairs:
        if uri == start:
            return True
    return False

def is_end(uri):
    for _,end in time_predicate_pairs:
        if uri == end:
            return True
    return False

def get_start_predicate(uri):
    for start,end in time_predicate_pairs:
        if uri == end:
            return start
    return None

def get_end_predicate(uri):
    for start,end in time_predicate_pairs:
        if uri == start:
            return end
    return None

def is_time_event(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p <http://rdf.freebase.com/ns/time.event>.
            }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    if(len(props)==1):
        return props[0] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    return False

def query_onehop_out_paths(uri):
    if (uri in onehop_relation_cache):
        return onehop_relation_cache[uri]["out_paths"]
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p WHERE {{
        <{}> ?p ?s.
        }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x:x["p"],results))
    return props

def get_statement_path_to_value(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX ns:<http://rdf.freebase.com/ns/>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p ?s0.
            ?s0  ns:measurement_unit.dated_integer.number ?num
            }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    return props

def query_onehop_in_paths(uri):
    if (uri in onehop_relation_cache):
        return onehop_relation_cache[uri]["in_paths"]
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p WHERE {{
        ?x ?p <{}>.
        }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x:x["p"],results))
    return props

def query_one_or_twohop_in_paths(uri):
    if(parse_uri(uri) in one_or_two_hop_paths_cache):
        return one_or_two_hop_paths_cache[parse_uri(uri)]["in_paths"]
    sparql = '''
    PREFIX ns:<http://rdf.freebase.com/ns/>
    select distinct ?p1 ?p2 where{{
        {{
            ?x ?p1 <{}>.
            filter(exists {{?x ns:type.object.name ?m}})
        }}
        UNION
        {{
            ?z ?p1 ?y.
            ?y ?p2 <{}>.
            filter(not exists {{?y ns:type.object.name ?m}})
            filter(?z != <{}>)
        }}
        filter(regex(?p1,"^http://rdf.freebase.com/ns/","i"))
        }}
    '''.format(uri,uri,uri)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        if "p2" in result.keys():
            paths.append([result["p1"],result["p2"]])
        else:
            paths.append([result["p1"]])
    return paths

def query_one_or_twohop_out_paths(uri):
    if (parse_uri(uri) in one_or_two_hop_paths_cache):
        return one_or_two_hop_paths_cache[parse_uri(uri)]["out_paths"]
    sparql = '''
    PREFIX ns:<http://rdf.freebase.com/ns/>
    select distinct ?p1 ?p2 where{{
        {{
            <{}> ?p1 ?x.
            filter(exists {{?x ns:type.object.name ?m}} || not exists {{?x ?n ?m}})
        }}
        UNION
        {{
            <{}> ?p1 ?y.
            ?y ?p2 ?z.
            filter(?z != <{}>)
            filter(not exists {{?y ns:type.object.name ?m}} )
        }}
        filter(?p1 != rdf:type && ?p1 != rdfs:label)
        filter(?p2 != rdf:type && ?p2 != rdfs:label)
        filter(?p1 != ns:type.object.type && ?p1 != ns:type.object.instance)
        filter(?p2 != ns:type.object.type && ?p2 != ns:type.object.instance)
        filter( !regex(?p1,"wikipedia","i"))
        filter( !regex(?p2,"wikipedia","i"))
        filter( !regex(?p1,"type.object","i"))
        filter( !regex(?p2,"type.object","i"))
        filter( !regex(?p1,"common.topic.","i"))
        filter( !regex(?p2,"common.topic.","i"))
        filter( !regex(?p1,"_id","i"))
        filter( !regex(?p2,"_id","i"))
        filter( !regex(?p1,"#type","i"))
        filter( !regex(?p2,"#type","i"))
        filter( !regex(?p1,"#label","i"))
        filter( !regex(?p2,"#label","i"))
        filter( !regex(?p1,"_id","i"))
        filter( !regex(?p2,"_id","i"))
        filter( !regex(?p1,"/ns/freebase.","i"))
        filter( !regex(?p2,"/ns/freebase.","i"))
        filter( !regex(?p1,"kg.","i"))
        filter( !regex(?p2,"kg.","i"))
        filter( regex(?p1,"^http://rdf.freebase.com/ns/","i"))
        }}
    '''.format(uri,uri,uri)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        if "p2" in result.keys():
            paths.append([result["p1"],result["p2"]])
        else:
            paths.append([result["p1"]])
    return paths

def query_one_or_twohop_in_paths_for_event(uri):
    sparql = '''
    PREFIX ns:<http://rdf.freebase.com/ns/>
    select distinct ?p1 ?p2 where{{
        {{
            ?x ?p1 <{}>.
            ?x ?q ?s.
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(exists {{?x ns:type.object.name ?m}})
        }}
        UNION
        {{
            ?z ?p1 ?y.
            ?y ?p2 <{}>.
            ?y ?q ?s.
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(not exists {{?y ns:type.object.name ?m}})
            filter(?z != <{}>)
        }}
        UNION
        {{
            ?z ?p1 ?y.
            ?y ?p2 <{}>.
            ?z ?q ?s.
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(not exists {{?y ns:type.object.name ?m}})
            filter(?z != <{}>)
        }}
        UNION
        {{
            ?z ?p1 ?y.
            ?y ?p2 <{}>.
            ?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.freebase.com/ns/time.event>.
            filter(?z != <{}>)
        }}
        filter(regex(?p1,"^http://rdf.freebase.com/ns/","i"))
        }}
    '''.format(uri,uri,uri,uri,uri,uri,uri)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        if "p2" in result.keys():
            paths.append([result["p1"],result["p2"]])
        else:
            paths.append([result["p1"]])
    return paths

def query_one_or_twohop_out_paths_for_event(uri):
    if (parse_uri(uri) in one_or_twohop_paths_for_event_cache):
        return one_or_twohop_paths_for_event_cache[parse_uri(uri)]["out_paths"]
    sparql = '''
    PREFIX ns:<http://rdf.freebase.com/ns/>
    select distinct ?p1 ?p2 where{{
        {{
            <{}> ?p1 ?x.
            ?x ?q ?s.
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(exists {{?x ns:type.object.name ?m}} || not exists {{?x ?n ?m}})
        }}
        UNION
        {{
            <{}> ?p1 ?y.
            ?y ?p2 ?z.
            ?y ?q ?s.
            filter(?z != <{}>)
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(not exists {{?y ns:type.object.name ?m}} )
        }}
        UNION
        {{
            <{}> ?p1 ?y.
            ?y ?p2 ?z.
            ?z ?q ?s.
            filter(?z != <{}>)
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            filter(not exists {{?y ns:type.object.name ?m}} )
        }}
        UNION
        {{
            <{}> ?p1 ?y.
            ?y ?p2 ?z.
            ?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.freebase.com/ns/time.event>.
            filter(?z != <{}>)
        }}
        filter(?p1 != rdf:type && ?p1 != rdfs:label)
        filter(?p2 != rdf:type && ?p2 != rdfs:label)
        filter(?p1 != ns:type.object.type && ?p1 != ns:type.object.instance)
        filter(?p2 != ns:type.object.type && ?p2 != ns:type.object.instance)
        filter( !regex(?p1,"wikipedia","i"))
        filter( !regex(?p2,"wikipedia","i"))
        filter( !regex(?p1,"type.object","i"))
        filter( !regex(?p2,"type.object","i"))
        filter( !regex(?p1,"common.topic.","i"))
        filter( !regex(?p2,"common.topic.","i"))
        filter( !regex(?p1,"_id","i"))
        filter( !regex(?p2,"_id","i"))
        filter( !regex(?p1,"#type","i"))
        filter( !regex(?p2,"#type","i"))
        filter( !regex(?p1,"#label","i"))
        filter( !regex(?p2,"#label","i"))
        filter( !regex(?p1,"_id","i"))
        filter( !regex(?p2,"_id","i"))
        filter( !regex(?p1,"/ns/freebase.","i"))
        filter( !regex(?p2,"/ns/freebase.","i"))
        filter( !regex(?p1,"kg.","i"))
        filter( !regex(?p2,"kg.","i"))
        filter( regex(?p1,"^http://rdf.freebase.com/ns/","i"))
        }}
    '''.format(uri,uri,uri,uri,uri,uri,uri)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        if "p2" in result.keys():
            paths.append([result["p1"],result["p2"]])
        else:
            paths.append([result["p1"]])
    return paths

def is_part_of_for_cvt(entity1,entity2):
    props = get_path_between_two_entity(entity1,entity2)
    res = []
    for result in props:
        if(len(result)==2):
            if(result[1] in part_of_predicates):
                res.append(result)
    return res

def is_part_of(entity1,entity2):
    props = get_path_between_two_entity(entity1, entity2)
    res = []
    for result in props:
        if (len(result) == 1):
            if (result[0] in part_of_predicates):
                res.append(result)
    return res

def query_relation_by_cvt(subject,object):
    sparql = """
    SELECT DISTINCT ?p1 ?p2 WHERE {{
        <{}> ?p1 ?y.
        ?y ?p2 <{}>.
        filter(not exists {{?y ns:type.object.name ?m}} )
        }}""".format(subject,object)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x:[x["p1"],x["p2"]],results))
    return props

def query_cvt_time_preds(prop1,prop2,subject,object):
    sparql = """
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT DISTINCT ?p WHERE {{
        <{}> <{}> ?y.
        ?y <{}> <{}>.
        ?y ?p ?o.
        }}""".format(subject,prop1,prop2,object)
    results = query_freebase_with_odbc(sparql)
    time_preds = []
    for result in results:
        if is_time_predicate(result["p"]):
            time_preds.append(result["p"])
    return time_preds

def get_onehop_path_to_time(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p WHERE {{
            <{}> ?p ?s.
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    return props

def get_onehop_path_to_time_event(uri):
    sparql = """
            PREFIX wikibase: <http://wikiba.se/ontology#>
            SELECT DISTINCT ?p WHERE {{
                <{}> ?p ?s.
                ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.freebase.com/ns/time.event>.
                }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    return props

def get_twohop_path_to_time_event(uri):
    sparql = """
            PREFIX wikibase: <http://wikiba.se/ontology#>
            SELECT DISTINCT ?p1 ?p2 WHERE {{
                <{}> ?p1 ?e.
                ?e ?p2 ?s.
                FILTER (strstarts(str(?e),"http://rdf.freebase.com/ns/"))
                ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.freebase.com/ns/time.event>.
                }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        paths.append([result["p1"], result["p2"]])
    return paths

def query_label(uri):
    if uri in label_dict:
        return label_dict[uri]
    sparql = "SELECT ?label WHERE {{ <{}> rdfs:label ?label.FILTER((lang(?label)=\"en\"))}}".format(uri)
    results = query_freebase_with_odbc(sparql)
    if len(results) > 0:
        label = results[0]["label"]
        label_dict[uri] = label
        return label
    else:
        return ""

def query_pair_label(start,end):
    start_label = query_label(start)
    end_label = query_label(end)
    return start_label + " and " + end_label

def is_cvt(uri):
    if not "m." in uri:
        return False
    sparql = "select ?x where {<" + uri + "> rdfs:label ?x }"
    results = query_freebase_with_odbc(sparql)
    if len(results) == 0:
        return True
    return False

def get_path_between_two_entity(entity1,entity2):
    sparql = '''
        PREFIX ns:<http://rdf.freebase.com/ns/>
        select distinct ?p1 ?p2 where{{
            {{
                <{}> ?p1 <{}>.
            }}
            UNION
            {{
                <{}> ?p1 ?y.
                ?y ?p2 <{}>.
            }}
            filter(?p1 != rdf:type && ?p1 != rdfs:label)
            filter(?p2 != rdf:type && ?p2 != rdfs:label)
            filter(?p1 != ns:type.object.type && ?p1 != ns:type.object.instance)
            filter(?p2 != ns:type.object.type && ?p2 != ns:type.object.instance)
            filter( !regex(?p1,"wikipedia","i"))
            filter( !regex(?p2,"wikipedia","i"))
            filter( !regex(?p1,"type.object","i"))
            filter( !regex(?p2,"type.object","i"))
            filter( !regex(?p1,"_id","i"))
            filter( !regex(?p2,"_id","i"))
            filter( !regex(?p1,"#type","i"))
            filter( !regex(?p2,"#type","i"))
            filter( !regex(?p1,"#label","i"))
            filter( !regex(?p2,"#label","i"))
            filter( !regex(?p1,"_id","i"))
            filter( !regex(?p2,"_id","i"))
            filter( !regex(?p1,"/ns/freebase.","i"))
            filter( !regex(?p2,"/ns/freebase.","i"))
            filter( !regex(?p1,"kg.","i"))
            filter( !regex(?p2,"kg.","i"))
            filter( regex(?p1,"^http://rdf.freebase.com/ns/","i"))
            }}
        '''.format(entity1, entity2, entity1, entity2)
    results = query_freebase_with_odbc(sparql)
    paths = []
    for result in results:
        if "p2" in result.keys() and result["p2"]:
            paths.append([result["p1"], result["p2"]])
        else:
            paths.append([result["p1"]])
    return paths

def get_time_around_entity(uri):
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?s WHERE {{
            {{
            <{}> ?f ?s.
            }}
            FILTER (datatype(?s)=xsd:dateTime)
            }}""".format(uri,uri)
    results = query_freebase(sparql)
    props = []
    for result in results:
        p = result["s"]
        props.append(p)
    return props

def get_twohop_path_to_time(uri):#cache
    if(parse_uri(uri) in twohop_path_to_time_cache):
        return twohop_path_to_time_cache[parse_uri(uri)]
    sparql = """
        PREFIX wikibase: <http://wikiba.se/ontology#>
        SELECT DISTINCT ?p ?q WHERE {{
            <{}> ?p ?e.
            ?e ?q ?s.
            FILTER (strstarts(str(?e),"http://rdf.freebase.com/ns/"))
            FILTER(isLiteral(?s))
            FILTER(DATATYPE(?s)=xsd:dateTime or DATATYPE(?s)=xsd:gYear or DATATYPE(?s)=xsd:date or DATATYPE(?s)=xsd:gYearMonth)
            }}""".format(uri)
    results = query_freebase_with_odbc(sparql)
    props = []
    for result in results:
        p1 = result["p"]
        p2 = result["q"]
        props.append([p1,p2])
    return props

def get_time_predicates_from_var(var,raw_sparql):
    index = raw_sparql.index("?"+var+".")+len("?"+var+".")
    sparql = "?"+var+" ?p ?time.?p <http://www.w3.org/2000/01/rdf-schema#range> <http://rdf.freebase.com/ns/type.datetime>."
    sparql = raw_sparql[:index]+sparql+raw_sparql[index:]
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    return props

def get_predicates_from_var_to_time_event(var,raw_sparql):
    index = raw_sparql.index("?" + var + ".") + len("?" + var + ".")
    sparql = "?" + var + " ?p ?time.?time <http://www.w3.org/2000/01/rdf-schema#type> <http://rdf.freebase.com/ns/time.event>."
    sparql = raw_sparql[:index] + sparql + raw_sparql[index:]
    results = query_freebase_with_odbc(sparql)
    props = list(map(lambda x: x["p"], results))
    return props

if __name__ == "__main__":
    freebase_init(DATASET.TEQ)
    print(query_one_or_twohop_out_paths_for_event("http://rdf.freebase.com/ns/m.010qcv"))
    print(is_part_of("http://rdf.freebase.com/ns/m.010bxwg5","http://rdf.freebase.com/ns/m.025b7qw"))
    # print(check_prefix("rdfs",get_uri("fb","m.02mjmr")))

    # el = []
    # with open("data/freebase/entity_in_TEQ.txt", "r", encoding="utf-8") as f:
    #     for line in f.readlines():
    #         el.append(line.strip())
    # onehop_relation_cache = {}
    # for i in el:
    #     out_path = query_onehop_out_paths(i)
    #     in_path = query_onehop_in_paths(i)
    #     onehop_relation_cache[i] = [out_path,in_path]
    # with open("data/freebase/onehop_relation_cache.json","w+",encoding="utf-8") as f:
    #     json.dump(onehop_relation_cache,fp=f)
