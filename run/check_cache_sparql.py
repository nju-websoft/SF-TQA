from utils.dataset_utils import load_query_caches,load_json_dataset
from evaluate.evaluate import Evaluator

def compare_float(a, b):
    if abs(a - b) <= 1e-6:
        return True
    return False

dataset = load_query_caches("data/dataset/TimeQuestions/graph_rank/test_cache_rp_new")
raw_dataset = load_json_dataset("data/dataset/TimeQuestions/test_prepared.json")
#print(len(dataset))
#dataset = load_query_caches("/home2/hchen/TimeQA/data/dataset/TimeQuestions/graph_rank/train_cache")
dataset_num = len(dataset)
print(dataset_num)
questions_answer_map = {}
for raw_sample in raw_dataset:
    questions_answer_map[raw_sample["question"]] = [raw_sample["id"],raw_sample["answers"]]

for sample in dataset:
    question = sample["question"]
    answers = questions_answer_map[question][1]
    graphs_with_score = sample["graphs_with_score"]
    if len(graphs_with_score) > 0:
        for graph_with_score in graphs_with_score:
            graph = graph_with_score["graph"]
            predicts = graph.get_answers()
            if len(predicts) ==0:
                print(question)
                print(questions_answer_map[question][0])
                break
            evaluator = Evaluator(predicts, answers)
            if not (compare_float(graph_with_score["f1"],evaluator.f1) and compare_float(graph_with_score["p"],evaluator.p) and compare_float(graph_with_score["r"],evaluator.r) ):
                print(question)##and compare_float(graph_with_score["hit1"],evaluator.hit1)
                print(questions_answer_map[question][0])
                break