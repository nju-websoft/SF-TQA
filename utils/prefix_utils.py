import re

prefix2url = {}
url2prefix = {}
id_regex = re.compile("^.*(?P<id>[P|Q][0-9]+)$")
with open("data/wikidata/prefix.txt","rt",encoding="utf-8") as fin:
    for line in fin:
        line = line.strip()
        prefix = line.split("\t")[0]
        url = line.split("\t")[1]
        prefix2url[prefix]=url
        url2prefix[url]=prefix

def check_prefix(uri,prefix):
    if uri.startswith("http://www.wikidata.org/"):
        match = id_regex.match(uri)
        wikidata_id = match.groupdict()["id"]
        uri_prefix = uri.split(wikidata_id)[0]
        if uri_prefix == prefix2url[prefix]:
            return True
        else:
            return False
    else:
        if uri.startswith(prefix2url[prefix]):
            return True
        else:
            return False

def parse_id(uri):
    if uri.startswith("Q") or uri.startswith("P"):
        return uri
    try:
        id = id_regex.match(uri).groupdict()["id"]
    except:
        print("l")
    return id

def parse_prefix(uri):
    id = parse_id(uri)
    prefix_uri = uri.split(id)[0]
    return prefix

def get_uri(name):
    if name.startswith("http"):
        return name
    if name.startswith("statement"):
        return "http://www.wikidata.org/entity/"+name
    prefix = name.split(":")[0]
    localname = name.split(":")[1]
    uri = prefix2url[prefix] + localname
    return uri
