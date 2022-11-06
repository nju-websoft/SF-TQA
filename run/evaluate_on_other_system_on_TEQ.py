from utils.dataset_utils import load_json_dataset
from evaluate.evaluate import Evaluator
from optparse import OptionParser
import random
import os

def parse_args():
    parser = OptionParser()
    parser.add_option("-m","--method",action='store',type='string',dest='method')
    opt,args = parser.parse_args()
    return opt,args

quint_predict_file = "data/dataset/TempQuestions/QUINT_on_TempQuestions.json"
aqqu_predict_file = "data/dataset/TempQuestions/AQQU_on_TempQuestions.json"
aqqu_and_tequila_predict_file = "data/dataset/TempQuestions/tempquestions-tequila-aqqu-detailed.json"
quint_and_tequila_predict_file = "data/dataset/TempQuestions/tempquestions-tequila-quint-detailed.json"
test_file = "data/dataset/TempQuestions/test_prepared.json"

def get_res_dict(method):
    if(method=="quint"):
        quint_results = load_json_dataset(quint_predict_file)
        quint_result_dict = {}
        for quint_result in quint_results:
            predicts = quint_result["QUINT answer"]
            predicts = [predict.split("(")[0].lower() for predict in predicts]
            question = quint_result["Question"].lower().replace("?", "")
            quint_result_dict[question] = predicts
        return quint_result_dict
    if(method=="aqqu"):
        aqqu_results = load_json_dataset(aqqu_predict_file)
        aqqu_result_dict = {}
        for aqqu_result in aqqu_results:
            predicts = aqqu_result["AQQU answer"]
            predicts = [predict.split("(")[0].lower() for predict in predicts]
            question = aqqu_result["Question"].lower().replace("?", "")
            aqqu_result_dict[question] = predicts
        return aqqu_result_dict
    if(method=="aqqu+tequila"):
        teq_results = load_json_dataset(aqqu_and_tequila_predict_file)
        teq_result_dict = {}
        for teq_result in teq_results:
            predicts = teq_result["AQQU+TEQUILA answer"]
            predicts = [predict.lower() for predict in predicts]
            question = teq_result["Question"].lower().replace("?", "")
            teq_result_dict[question] = predicts
        return teq_result_dict
    if(method=="quint+tequila"):
        teq_results = load_json_dataset(quint_and_tequila_predict_file)
        teq_result_dict = {}
        for teq_result in teq_results:
            predicts = teq_result["QUINT+TEQUILA answer"]
            predicts = [predict.lower() for predict in predicts]
            question = teq_result["Question"].lower().replace("?", "")
            teq_result_dict[question] = predicts
        return teq_result_dict

if __name__ == "__main__":
    random.seed(42)
    os.environ['PYTHONHASHSEED'] = '42'

    opt, _ = parse_args()
    map_system_result_dict = get_res_dict(opt.method)

    dataset = load_json_dataset(test_file)
    avg_p = 0.0
    avg_r = 0.0
    avg_f1 = 0.0
    avg_hit1 = 0.0
    for sample in dataset:
        answers = sample["answers"]
        answers = [answer.lower() for answer in answers]
        question = sample["question"].lower().replace("?", "")
        id = sample["id"]
        print("question {} : {}".format(id, question))
        print("answers:{}".format(answers))
        predicts = map_system_result_dict[question]
        if (predicts == ['']):
            predicts = []
        print("predicts:{}".format(predicts))
        evaluator = Evaluator(predicts,answers)
        print("question P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(evaluator.p, evaluator.r, evaluator.f1,
                                                                        evaluator.hit1))
        avg_p += evaluator.p
        avg_r += evaluator.r
        avg_f1 += evaluator.f1
        avg_hit1 += evaluator.hit1
        print()
    avg_p /= len(dataset)
    avg_r /= len(dataset)
    avg_f1 /= len(dataset)
    avg_hit1 /= len(dataset)

    print("average P:{:.3f} R:{:.3f} F1:{:.3f} Hit1:{:.3f}".format(avg_p,avg_r,avg_f1,avg_hit1))
