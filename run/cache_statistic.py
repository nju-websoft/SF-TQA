from utils.dataset_utils import DATASET,load_query_caches,load_parsed_questions

dataset = load_query_caches("/home2/hchen/TimeQA/data/dataset/TimeQuestions/graph_rank/train_cache")
dataset_num = len(dataset)
print(dataset_num)
max_f1 = 0.
max_p = 0.
max_r = 0.
max_hit1 = 0.
avg_candidates = 0
for sample in dataset:
    if len(sample["graphs_with_score"]) > 0:
        max_f1 += sample["graphs_with_score"][0]["f1"]
        max_p += sample["graphs_with_score"][0]["p"]
        max_r += sample["graphs_with_score"][0]["r"]
        max_hit1 += sample["graphs_with_score"][0]["hit1"]
        avg_candidates += len(sample["graphs_with_score"])
    else:
        max_p += 1

print("uper bound p:{} r:{} f1:{} hit1:{}".format(max_p/dataset_num,max_r/dataset_num,max_f1/dataset_num,max_hit1/dataset_num))
print("avg candiates:{}".format(avg_candidates/dataset_num))
