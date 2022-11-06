from evaluate.evaluate import Evaluator
from utils.dataset_utils import DATASET, load_json_dataset, load_query_caches
from rank.ranker import BertRanker
from optparse import OptionParser
from utils.freebase_utils import freebase_init
from utils.wikidata_utils import wikidata_init
import numpy as np
import torch

def compare_float(a, b):
    if abs(a - b) <= 0.05:
        return True
    return False

def parse_args():
    parser = OptionParser()
    parser.add_option("-i","--input_path",action='store',type='string',dest='input_path',default="data/dataset/TimeQuestions/graph_rank/test_cache")
    parser.add_option("-d",action="store",type="string",dest="device",default="cuda:3")
    parser.add_option("-g", "--ground_kb", action="store", type="string", dest="ground_kb",default="WIKIDATA")
    parser.add_option("-r", "--raw_dataset_path", action='store', type='string', dest='dataset_path',
                      default="data/dataset/TimeQuestions/test_prepared1.json")
    parser.add_option("--model_path",action="store",type="string",dest="model_path",default="data/models/graph_rank_tq_best_neg25_3090_rp.pth")
    opt,args = parser.parse_args()
    return opt,args

def get_question_ans_map(raw_dataset):
    questions_answer_map = {}
    for raw_sample in raw_dataset:
        questions_answer_map[raw_sample["question"]] = [raw_sample["id"], raw_sample["answers"]]
    return questions_answer_map

def calculate_metric(dataset,ranker,questions_answer_map,ground_kb):
    from rank.wikidata_tag import WikidataTagger
    from rank.freebase_tag import FreebaseTagger
    if ground_kb == "FREEBASE":
        freebase_init(DATASET.TEQ)
        tagger = FreebaseTagger()
    else:
        wikidata_init(DATASET.TQ)
        tagger = WikidataTagger()
    avg_p,avg_r,avg_f1,avg_hit1 = 0.0,0.0,0.0,0.0
    for sample in dataset:
        question = sample["question"]
        id = questions_answer_map[question][0]
        answers = questions_answer_map[question][1]
        print("question {} : {}".format(id, question))
        print("answers:{}".format(answers))
        graphs_with_score = sample["graphs_with_score"]
        if len(graphs_with_score) == 0:
            print("predicts:[]")
            print("query_graph:")
            print("sparql:")
            evaluator = Evaluator([], answers)
            print("question P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(evaluator.p, evaluator.r, evaluator.f1,
                                                                            evaluator.hit1))
            print()
            avg_p += 1
            continue
        labels = [tagger.tag(graph_with_score["graph"]) for graph_with_score in graphs_with_score]
        scores = ranker.rank(question,labels)
        predict_index = np.argmax(scores)
        predict_graph = graphs_with_score[predict_index]
        graph = predict_graph["graph"]
        predicts = graph.get_answers()
        print("predicts:{}".format(predicts))
        print("target graph frame name:", graph.frame_name)
        print("query_graph:", tagger.tag(graph))
        print("sparql:", graph.to_sparql())
        evaluator = Evaluator(predicts, answers)
        # print("question P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(evaluator.p, evaluator.r, evaluator.f1,
        #                                                                 evaluator.hit1))
        if not (compare_float(predict_graph["f1"], evaluator.f1) and compare_float(predict_graph["p"],
                                                                                      evaluator.p) and compare_float(
                predict_graph["r"], evaluator.r)):
            print(str(id) + "need re cache")
        print("question P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(predict_graph["p"], predict_graph["r"], predict_graph["f1"],
                                                                        predict_graph["hit1"]))
        print()
        avg_f1 += predict_graph["f1"]
        avg_p += predict_graph["p"]
        avg_r += predict_graph["r"]
        avg_hit1 += predict_graph["hit1"]
    avg_p /= len(dataset)
    avg_r /= len(dataset)
    avg_f1 /= len(dataset)
    avg_hit1 /= len(dataset)
    return avg_p,avg_r,avg_f1,avg_hit1


if __name__ == "__main__":
    opt,_ = parse_args()
    ranker = BertRanker(opt.model_path,device=torch.device(opt.device))
    dataset = load_query_caches(opt.input_path)
    raw_dataset = load_json_dataset(opt.dataset_path)
    p,r,f1,hit1 = calculate_metric(dataset,ranker,get_question_ans_map(raw_dataset),opt.ground_kb)
    print("Overall:")
    print("avg p:{} r:{} f1:{} hit1:{}".format(p,r,f1,hit1))