from optparse import OptionParser
from utils.dataset_utils import dump_json_dataset, load_json_dataset
from utils.nlp_utils import parse_ordinal
import re
import copy

numerical_pattern = re.compile(r"^-?[0-9]+(\.[0-9]+)?$")
thousand_pattern = re.compile(r"^[0-9]+(,[0-9]{3,3})+$")
def parse_args():
    parser = OptionParser()
    parser.add_option('-i',dest="input_path",type=str,help="input path")
    parser.add_option('-o',dest="output_path",type=str,help="output path")
    opt,args = parser.parse_args()
    return opt,args

def parse_temporal_ans(labels):
    temporal_ans_cons = None
    for label in labels:
        if label["label"] == "T2":
            temporal_ans_cons = {'signal':label}
            break
    return temporal_ans_cons

def parse_temporal_ordinals(labels):
    temporal_ordinals = []
    signal = None
    for label in labels:
        if label["label"] == "S2":
            rank = parse_ordinal(label["mention"])
            if rank is not None:
                signal = label
                break
    if signal is not None:
        target_event = None
        for label in labels:
            if label["label"].startswith("E"):
                target_event = label
                break
        temporal_ordinal = {"signal":signal,"rank":parse_ordinal(signal["mention"]),"target":target_event}
        temporal_ordinals.append(temporal_ordinal)
    return temporal_ordinals

def parse_numerical_cmp_cons(question,labels,entity_mentions):
    if question.find("series ordinal is") != -1:
        return []
        
    less_than_triggers = ["less than"]
    greater_than_triggers = ["greater than","more than"]

    numbers = []
    number_cons_list = []
    question_tokens = question.split()

    for token in question_tokens:
        if numerical_pattern.match(token):
            add_flag = True
            for label in labels:
                if token in label["mention"] or label["mention"] in token:
                    add_flag = False
                    break
            for entity_mention in entity_mentions:
                if token in entity_mention or entity_mention in token:
                    add_flag = False
                    break
            if add_flag:
                numbers.append(token)
        if thousand_pattern.match(token):
            numbers.append(token)

    for number in numbers:
        equals_flag = True
        for trigger in less_than_triggers:
            if trigger + " " + number in question:
                if "," in number:
                    number = "".join(number.split(","))
                number_cons = {"cmp_sign":"<=","number":number}
                number_cons_list.append(number_cons)
                equals_flag = False
        
        for trigger in greater_than_triggers:
            if trigger + " " + number in question:
                if "," in number:
                    number = "".join(number.split(","))
                number_cons = {"cmp_sign":">=","number":number}
                number_cons_list.append(number_cons)
                equals_flag = False
        
        if equals_flag:
            if "," in number:
                number = "".join(number.split(","))
            number_cons = {"cmp_sign":"=","number":number}
            number_cons_list.append(number_cons)

    return number_cons_list

def enrich_temporal_ordinal(question,labels):
    triggers = ["currently","now","current","today","present"]
    for label in labels:
        if label["label"] == "T1" and label["mention"].replace("?","").lower().strip() not in triggers:
            return []
    question_tokens = question.replace("?","").split()
    for token in question_tokens:
        if token in triggers:
            start = question.find(token)
            label = {"id":-1,"label":"S2","mention":token,"start":start,"end":start+len(token)}
            temporal_ordinal_cons = [{"rank":-1,"signal":label,"target":None}]
            return temporal_ordinal_cons
            
    if "series ordinal is" in question:
        start = question.find("series ordinal is")
        rank = question[start + len("series ordinal is"):].strip().split()[0]
        start = question.find(rank)
        label = {"id":-1,"label":"S2","mention":rank,"start":start,"end":start+len(rank)}
        temporal_ordinal_cons = [{"rank":int(rank),"signal":label,"target":None}]
        return temporal_ordinal_cons
    return []

inverse_relation = {
    "INCLUDES":"IS_INCLUDED",
    "IS_INCLUDED":"INCLUDES",
    "BEGINS":"BEGUN_BY",
    "BEGUN_BY":"BEGINS",
    "ENDS":"ENDED_BY",
    "ENDED_BY":"ENDS",
    "SIMULTANEOUS":"SIMULTANEOUS",
    "AFTER":"BEFORE",
    "BEFORE":"AFTER",
    "IAFTER":"IBEFORE",
    "IBEFORE":"IAFTER"
}

def get_format_relations(relations):
    format_relations = []
    for relation in relations:
        format_relation = copy.deepcopy(relation)
        #set E2 E3 as target
        if format_relation["related_to"]["label"] == "E2" or format_relation["related_to"]["label"] == "E3":
            tmp = format_relation["related_to"]
            format_relation["related_to"] = format_relation["target"]
            format_relation["target"] = tmp
            format_relation["rel_type"] = inverse_relation[format_relation["rel_type"]]
        #simplify relations
        if format_relation["rel_type"] == "BEGINS" or format_relation["rel_type"] == "BEGUN_BY":
            format_relation["rel_type"] = "BEGIN"
        elif format_relation["rel_type"] == "ENDS" or format_relation["rel_type"] == "ENDED_BY":
            format_relation["rel_type"] = "END"
        elif format_relation["rel_type"]== "INCLUDES" or format_relation["rel_type"] == "IS_INCLUDED":
            format_relation["rel_type"] = "INCLUDE"
        if format_relation["related_to"]["label"] == "T1" and format_relation["related_to"]["interval"] is None:
            continue
        format_relations.append(format_relation)
    return format_relations

def preprocess(dataset):
    format_dataset = []
    for sample in dataset:
        format_sample = {"id":str(sample["id"]),"question":sample["question"]}
        format_sample["answers"] = sample["answers"]
        format_sample["entity_linking"] = sample["entity_linking"]
        format_sample["labels"] = sample["labels"]
        format_sample["temporal_relations"] = sample["temporal_relations"]

        #extract data structure from raw temporal semantic provided by utils
        format_sample["rec_temporal_ans_cons"] = parse_temporal_ans(sample["labels"])
        format_sample["rec_temporal_relations"] = get_format_relations(sample["temporal_relations"])
        format_sample["rec_temporal_ordinals"] = parse_temporal_ordinals(sample["labels"])
        format_sample["rec_temporal_ordinals"] += enrich_temporal_ordinal(sample["question"],sample["labels"])
        format_sample["rec_numerical_cmp_cons"] = parse_numerical_cmp_cons(sample["question"],sample["labels"],format_sample["entity_linking"].keys())
        
        format_dataset.append(format_sample)

    return format_dataset

if __name__ == "__main__":
    opt,_ = parse_args()
    dataset = load_json_dataset(opt.input_path)
    format_dataset = preprocess(dataset)
    print(len(format_dataset))
    dump_json_dataset(format_dataset,opt.output_path)