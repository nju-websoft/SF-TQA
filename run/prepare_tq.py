from optparse import OptionParser
from utils.dataset_utils import dump_json_dataset, load_json_dataset
from utils.sutime_utils import annotate_datetime

def parse_args():
    parser = OptionParser()
    parser.add_option('-i',dest="input_path",type=str,help="input path")
    parser.add_option('-a',dest="annotate_path",type=str,help="annotate path")
    parser.add_option('-e',dest="el_path",type=str,help="entity linking path")
    parser.add_option('-o',dest="output_path",type=str,help="output path")
    opt,args = parser.parse_args()
    return opt,args

def load_id2el(el_file):
    data = load_json_dataset(el_file)
    id2el = {}
    for js in data:
        id2el[js["id"]] = js["entity_linking"]
    return id2el

def parse_temporal_relations(annotate):
    temporal_relations = []
    if "relations" in annotate.keys():
        id2label = {}
        for label in annotate["labels"]:
            id2label[label["id"]] = label
        for relation in annotate["relations"]:
            temporal_relation = {}
            if "signal" in relation.keys():
                temporal_relation["signal"] = id2label[relation["signal"]]
            temporal_relation["target"] = id2label[relation["target"]]
            temporal_relation["related_to"] = id2label[relation["relatedTo"]]
            if temporal_relation["related_to"]["label"] == "T1" and temporal_relation["related_to"]["interval"] is None:
                continue
            temporal_relation["type"] = relation["type"]
            temporal_relation["type"] = relation["type"]
            temporal_relation["rel_type"] = relation["relType"]
            temporal_relations.append(temporal_relation)
    return temporal_relations

if __name__ == "__main__":
    opt,_ = parse_args()
    dataset = load_json_dataset(opt.input_path)
    annotates = load_json_dataset(opt.annotate_path)
    id2el = load_id2el(opt.el_path)
    question2annotate = {annotate["question"]:annotate for annotate in annotates}
    format_dataset = []
    for sample in dataset:
        format_sample = {"id":sample["Id"],"question":sample["Question"]}
        answers = [answer["WikidataQid"] if answer["AnswerType"] == "Entity" else answer["AnswerArgument"] for answer in sample["Answer"]]
        answers = ["-" + answer[2:] if answer.startswith("-0") and "T" in answer else answer for answer in answers]
        format_sample["answers"] = answers
        format_sample["entity_linking"] = id2el[sample["Id"]]
        format_sample["labels"] = []
        format_sample["temporal_relations"] = []

        if sample["Question"] in question2annotate.keys():
            annotate = question2annotate[sample["Question"]]
            time_annotates = annotate_datetime(sample["Question"],ref_date='2020-04-01')
            if "labels" in annotate.keys():
                for label in annotate["labels"]:
                    if label["label"] == "T1":
                        label["interval"] = None
                        for time_annotate in time_annotates:
                            if label["mention"] in time_annotate["text"] or time_annotate["text"] in label["mention"]:
                                label["interval"] = time_annotate["interval"]
                format_sample["labels"] = annotate["labels"]                    
            format_sample["temporal_relations"] = parse_temporal_relations(annotate)
        format_dataset.append(format_sample)
    print(len(format_dataset))
    dump_json_dataset(format_dataset,opt.output_path)