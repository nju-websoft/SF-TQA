

class Link():
    def __init__(self,type,uri,score,mention):
        self.type = type
        self.uri = uri
        self.score = score
        self.mention = mention

class Label():
    def __init__(self,label_json):
        self.id = label_json["id"]
        #E1,E2,E3,S1,S2,T1,T2
        self.type = label_json["label"]
        self.text = label_json["mention"]
        self.startpos = label_json["start"]
        self.endpos = label_json["end"]
        self.interval = None
        if "interval" in label_json.keys():
            self.interval = label_json["interval"]

class TemporalRelation():
    def __init__(self,relation_json):
        self.type = relation_json["type"]
        self.rel_type = relation_json["rel_type"]
        self.signal = None
        self.target = Label(relation_json["target"])
        self.related = Label(relation_json["related_to"])
        if "signal" in relation_json.keys() and relation_json["signal"] is not None:
            self.signal = Label(relation_json["signal"])
        if self.rel_type is None:
            self.rel_type = "INCLUDE"

class TemporalOrdinal():
    def __init__(self,ordinal_json):
        self.signal = Label(ordinal_json["signal"])
        self.target = None
        self.rank = ordinal_json["rank"]
        if "target" in ordinal_json.keys() and ordinal_json["target"] is not None:
            self.target = Label(ordinal_json["target"])

class TemporalAnsCons():
    def __init__(self,temporal_ans_cons_json):
        self.signal = Label(temporal_ans_cons_json["signal"])

class NumericalCons():
    def __init__(self,numeric_cons_json):
        self.cmp_sign = numeric_cons_json["cmp_sign"]
        self.number = numeric_cons_json["number"]

class ParsedQuestion():
    def __init__(self, dataset_type, ground_kb, question_id, question, answers, entity_linking, labels,temporal_relations,
    rec_temporal_ans_cons=None, rec_temporal_relations=[], rec_temporal_ordinals=[], rec_numerical_cmp_cons=[]):
        self.dataset_type = dataset_type
        self.ground_kb = ground_kb
        self.id = question_id
        self.question = question
        self.answers = answers
        self.entity_linking = entity_linking
        #raw temporal semantic recoginized by utils
        self.labels = labels
        self.temporal_relations = temporal_relations

        #data structure extracted according to raw temporal semantic
        self.rec_temporal_ans_cons = rec_temporal_ans_cons
        self.rec_temporal_relations = rec_temporal_relations
        self.rec_temporal_ordinals = rec_temporal_ordinals
        self.rec_numerical_cmp_cons_list = rec_numerical_cmp_cons
