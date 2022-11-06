import enum
import os
import json
import pickle
import random
from semantic import *
from enum import Enum

DATASET = Enum('DATASET', 'CRQ TQ CQ TEQ')

def load_query_caches(cache_dir):
    if not os.path.exists(cache_dir):
        raise Exception("cache dir does not exist")
    sample_files = []
    for root,dirs,files in os.walk(cache_dir):
        for file in files:
            if file.endswith(".pkl"):
                sample_files.append(file)
    sample_files.sort(key=lambda f:int(f[:-4]))
    dataset = []
    for file in sample_files:
        file_path = os.path.join(cache_dir,file)
        sample = load_pickle_dataset(file_path)
        dataset.append(sample)
    return dataset

def load_parsed_questions(file, dataset_type, ground_kb):
    def parse_entity_linking(el_json):
        entity_linking = {}
        for mention, link_json in el_json.items():
            uri = link_json["value"]
            score = float(link_json["score"])
            link = Link("ENTITY", uri, score, mention)
            entity_linking[mention] = link
        return entity_linking

    dataset_json = load_json_dataset(file)
    if dataset_type == DATASET.TQ:
        from utils.preprocess_tq import preprocess
        dataset_json = preprocess(dataset_json)
    elif dataset_type == DATASET.TEQ:
        from utils.preprocess_teq import preprocess
        dataset_json = preprocess(dataset_json)

    dataset = []
    for sample in dataset_json:
        question_id = sample["id"]
        question = sample["question"]
        answers = sample["answers"]
        entity_linking = parse_entity_linking(sample["entity_linking"])
        labels = [Label(label) for label in sample["labels"]]
        temporal_relations = [TemporalRelation(relation_json) for relation_json in sample["temporal_relations"]]

        rec_temporal_answer_cons = None
        if sample["rec_temporal_ans_cons"] is not None:
            rec_temporal_answer_cons = TemporalAnsCons(sample["rec_temporal_ans_cons"])
        rec_temporal_relations = [TemporalRelation(relation_json) for relation_json in sample["rec_temporal_relations"]]
        rec_temporal_ordinals = [TemporalOrdinal(ordinal_json) for ordinal_json in sample["rec_temporal_ordinals"]]
        rec_numerical_cmp_cons = [NumericalCons(numerical_cons_json) for numerical_cons_json in
                              sample["rec_numerical_cmp_cons"]]
        parsed_question = ParsedQuestion(dataset_type, ground_kb, question_id, question, answers, entity_linking, \
                                         labels, temporal_relations, rec_temporal_answer_cons, rec_temporal_relations, \
                                         rec_temporal_ordinals, rec_numerical_cmp_cons)
        dataset.append(parsed_question)
    return dataset


def load_pickle_dataset(file):
    with open(file, "rb") as fin:
        dataset = pickle.load(fin)
        return dataset


def dump_pickle_dataset(dataset, file):
    with open(file, "wb") as fout:
        pickle.dump(dataset, fout)


def load_json_dataset(file):
    with open(file, "rt", encoding="utf-8") as fin:
        dataset = json.load(fin)
        return dataset


def dump_json_dataset(dataset, file):
    with open(file, "wt", encoding="utf-8") as fout:
        json.dump(dataset, fout, ensure_ascii=False, indent=4)


def change_data_format_for_json(item):
    for key, value in item.items():
        if (type(value) == set):
            item[key] = list(value)
    return item


def sample_for_crq(q_list, outpath):
    res = []
    others = []
    random.seed(5)
    template_map = dict()
    for i in q_list:
        tp = i["template"]
        if (template_map.get(tp)):
            template_map[tp].append(change_data_format_for_json(i))
        else:
            template_map[tp] = []
            template_map[tp].append(change_data_format_for_json(i))
    for i in template_map.values():
        random.shuffle(i)
        l = len(i)
        res += i[:int(l * 0.1)]
        others += i[int(l * 0.1):int(l * 0.1) + 1]
    other_num = len(q_list) * 0.1 - len(res)
    random.shuffle(others)
    res += others[:int(other_num)]
    with open(outpath, "w+", encoding="utf-8") as f:
        f.write(json.dumps(res, indent=2))


if __name__ == "__main__":
    # dataset = load_pickle_dataset("data/dataset/CronQuestions/train.pickle")
    # sample_for_crq(dataset, "data/dataset/CronQuestions/train.json")
    # print(dataset[0].question)
    '''
    dataset = load_parsed_questions("data/dataset/TQ/train_prepared1.json",DATASET.CRQ,ground_kb="WIKIDATA")
    for i,sample in enumerate(dataset):
        if i == 20:
            break
        print(sample.tag_question)
    '''
    #dataset = load_query_caches('data/dataset/TempQuestions/graph_rank/train_cache')
    #print(dataset[0]["graphs_with_score"][0])
    #for sample in dataset:
        #print(len(sample["graphs"]))
    
    dataset = load_parsed_questions('data/dataset/TimeQuestions/train_prepared1.json',DATASET.TQ,"WIKIDATA")
    print(dataset[0].question)
