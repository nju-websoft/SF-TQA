from evaluate.evaluate import Evaluator
from utils.dataset_utils import DATASET, load_parsed_questions
from solve.solve import TemporalQuestionSolver
from utils.wikidata_utils import wikidata_init, write_cache
from utils.freebase_utils import freebase_init, write_freebase_cache
from optparse import OptionParser
import time
import random
import os

def parse_args():
    parser = OptionParser()
    parser.add_option("-i","--input_path",action='store',type='string',dest='input_path')
    parser.add_option("-o","--output_path",action="store",type="string",dest="output_path")
    parser.add_option("-d","--dataset",action='store',type='string',dest='dataset')
    parser.add_option("-g","--ground_kb",action="store",type="string",dest="ground_kb")
    parser.add_option("--path_rank_model_path",action="store",type="string",dest="path_rank_model_path")
    parser.add_option("--graph_rank_model_path",action="store",type="string",dest="graph_rank_model_path")
    opt,args = parser.parse_args()
    return opt,args


if __name__ == "__main__":

    random.seed(42)
    os.environ['PYTHONHASHSEED']='42'

    opt, _ = parse_args()
    if opt.ground_kb == "WIKIDATA":
        wikidata_init(DATASET[opt.dataset])
    elif opt.ground_kb == "FREEBASE":
        freebase_init(DATASET[opt.dataset])
        
    dataset = load_parsed_questions(opt.input_path,DATASET[opt.dataset],opt.ground_kb)
    solver = TemporalQuestionSolver(opt.ground_kb,opt.path_rank_model_path,opt.graph_rank_model_path)
    sum_P = 0.0
    sum_R = 0.0
    sum_f1 = 0.0
    sum_hit1 = 0.0
    for i,parsed_question in enumerate(dataset):
        print("question {} : {}".format(i,parsed_question.question))
        print("answers:{}".format(parsed_question.answers))
        starttime = time.time()
        predicts = solver.solve(parsed_question)
        endtime = time.time()
        print(endtime-starttime)
        print("predicts:{}".format(predicts))
        evaluator = Evaluator(predicts,parsed_question.answers)
        print("question P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(evaluator.p,evaluator.r,evaluator.f1,evaluator.hit1))
        sum_P += evaluator.p
        sum_R += evaluator.r
        sum_f1 += evaluator.f1
        sum_hit1 += evaluator.hit1
        print("average P:{:.4f} R:{:.4f} F1:{:.4f} hit1:{:.4f}".format(sum_P/(i+1),sum_R/(i+1),sum_f1/(i+1),sum_hit1/(i+1)))
        print()
    if opt.ground_kb == "WIKIDATA":
        write_cache()
    elif opt.ground_kb == "FREEBASE":
        write_freebase_cache()