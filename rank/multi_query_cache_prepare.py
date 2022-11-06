from evaluate.evaluate import Evaluator
from utils.dataset_utils import *
from solve.generate import TemporalQueryGenerator
from optparse import OptionParser
from utils.freebase_utils import freebase_init,write_freebase_cache
from utils.wikidata_utils import wikidata_init,write_cache
import utils.freebase_utils as fb
import utils.wikidata_utils as wd
from query.querygraph import QueryGraph
import traceback
import os
import shutil
from multiprocessing import Process,Queue

def generate_graphs_for_wikidata(parsed_question):
    entity_linking = parsed_question.entity_linking
    entities = [link.uri for link in entity_linking.values()]
    graphs = []
    for entity in entities:
        out_paths = wd.query_onehop_out_paths(entity)
        for out_path in out_paths:
            graph = QueryGraph(parsed_question)
            graph.add_triple(entity,out_path[0],"s0")
            graph.add_triple("s0",out_path[1],"ans")
            graph.add_answer_var("ans")
            graphs.append(graph)

        #enumerate answer is time interval
        for out_path1 in out_paths:
            for out_path2 in out_paths:
                if out_path1[0] == out_path2[0] and wd.is_time_pair_predicate(out_path1[1],out_path2[1]):
                    graph = QueryGraph(parsed_question)
                    graph.add_triple(entity,out_path1[0],"s0")
                    graph.add_time_interval("s0",[out_path1[1],out_path2[1]],"ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)
                else:
                    pid1 = wd.parse_id(out_path1[0])
                    pid2 = wd.parse_id(out_path2[0])
                    if wd.is_time_pair_predicate(pid1,pid2):
                        graph = QueryGraph(parsed_question)
                        prop1 = wd.get_uri("wdt:" + pid1)
                        prop2 = wd.get_uri("wdt:" + pid2)
                        graph.add_time_interval(entity,[prop1,prop2],"ans")
                        graph.add_answer_var("ans")
                        graphs.append(graph)

        in_paths = wd.query_onehop_in_paths(entity)
        for in_path in in_paths:
            graph = QueryGraph(parsed_question)
            graph.add_triple("ans",in_path[1],"s0")
            graph.add_triple("s0",in_path[0],entity)
            graph.add_answer_var("ans")
            graphs.append(graph)

        out_paths, has_statement_type = wd.query_out_paths_for_event_statement(entity)
        for out_path in out_paths:
            graph = QueryGraph(parsed_question)
            graph.add_triple(entity, out_path[0], "s0")
            graph.add_triple("s0", out_path[1], "ans")
            if (has_statement_type):
                graph.set_statement_type("s0")
            graph.add_answer_var("ans")
            graphs.append(graph)
        in_paths, has_statement_type = wd.query_in_paths_for_event_statement(entity)
        for in_path in in_paths:
            graph = QueryGraph(parsed_question)
            graph.add_triple("ans", in_path[1], "s0")
            graph.add_triple("s0", in_path[0], entity)
            if (has_statement_type):
                graph.set_statement_type("s0")
            graph.add_answer_var("ans")
            graphs.append(graph)
        paths = wd.get_twohop_path_with_time_event(entity, "s0")
        for path in paths:
            graph = QueryGraph(parsed_question)
            graph.add_triple(path[0], path[1], path[2])
            graph.add_triple(path[3], path[4], path[5])
            graph.add_answer_var("s0Aspect")
            graphs.append(graph)
    return graphs

def generate_graphs_for_freebase(parsed_question):
        entity_linking = parsed_question.entity_linking
        entities = [link.uri for link in entity_linking.values()]
        graphs = []
        for entity in entities:
            out_paths = fb.query_one_or_twohop_out_paths(entity)
            for out_path in out_paths:
                if not out_path[0].startswith("http"):
                    print(out_path)
                if len(out_path) == 1:
                    graph = QueryGraph(parsed_question)
                    graph.add_triple(entity,out_path[0],"ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)
                else:
                    graph = QueryGraph(parsed_question)
                    graph.add_triple(entity,out_path[0],"c0")
                    graph.add_triple("c0",out_path[1],"ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)

            #enumerate answer is time interval
            for out_path1 in out_paths:
                for out_path2 in out_paths:
                    if len(out_path1) == 2 and len(out_path2) == 2 \
                        and out_path1[0] == out_path2[0] and fb.is_time_pair_predicate(out_path1[1],out_path2[1]):
                        graph = QueryGraph(parsed_question)
                        graph.add_triple(entity,out_path1[0],"c0")
                        graph.add_time_interval("c0",[out_path1[1],out_path2[1]],"ans")
                        graph.add_answer_var("ans")
                        graphs.append(graph)
                    elif len(out_path1) == 1 and len(out_path2) == 1 \
                        and fb.is_time_pair_predicate(out_path1[0],out_path2[0]):
                            graph = QueryGraph(parsed_question)
                            graph.add_time_interval(entity,[out_path1[0],out_path2[0]],"ans")
                            graph.add_answer_var("ans")
                            graphs.append(graph)

            out_paths = fb.query_one_or_twohop_out_paths_for_event(entity)
            for out_path in out_paths:
                if not out_path[0].startswith("http"):
                    print(out_path)
                if len(out_path) == 1:
                    graph = QueryGraph(parsed_question)
                    graph.add_triple(entity, out_path[0], "ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)
                else:
                    graph = QueryGraph(parsed_question)
                    graph.add_triple(entity, out_path[0], "c0")
                    graph.add_triple("c0", out_path[1], "ans")
                    graph.add_answer_var("ans")
                    graphs.append(graph)
        return graphs

def prepare_path_rank(process_id,queue,ground_kb,save_dir=""):
    if opt.ground_kb == "WIKIDATA":
        wikidata_init(DATASET[opt.dataset])
    elif opt.ground_kb == "FREEBASE":
        freebase_init(DATASET[opt.dataset])

    avg_max_f1 = 0.0
    total = 0
    while True:
        parsed_question = queue.get()
        if parsed_question is None:
            break
        print("question {}:{}".format(parsed_question.id,parsed_question.question))
        try:
            if ground_kb == "FREEBASE":
                graphs = generate_graphs_for_freebase(parsed_question)
            elif ground_kb == "WIKIDATA":
                graphs = generate_graphs_for_wikidata(parsed_question)
            rank_sample = {"question":parsed_question.question,"graphs_with_score":[]}
            total+=1
            graphs_with_score = rank_sample["graphs_with_score"]
            for graph in graphs:
                predicts = graph.get_answers()
                evaluator = Evaluator(predicts,parsed_question.answers)
                graph_with_score = {"graph":graph,"p":evaluator.p,"r":evaluator.r,"f1":evaluator.f1,"hit1":evaluator.hit1}
                graphs_with_score.append(graph_with_score)
            if len(graphs_with_score) > 0:
                graphs_with_score.sort(key=lambda g:g["f1"],reverse=True)             
                print("max F1:{}".format(graphs_with_score[0]["f1"]))
                print("query graph:{}".format(graphs_with_score[0]["graph"].to_label_seq()))
                print("sparql:{}".format(graphs_with_score[0]["graph"].to_sparql()))
                avg_max_f1 += graphs_with_score[0]["f1"]
        except Exception as err:
            print(err)
            traceback.print_exc()
            continue
        save_path =  os.path.join(save_dir,parsed_question.id+".pkl")
        dump_pickle_dataset(rank_sample,save_path)
    write_cache()
    if total > 0:
        avg_max_f1 /= total
    print("process id:{} avg max F1:{:.4f}".format(process_id,avg_max_f1))
    print("process id:{} finish".format(process_id))

def prepare_graph_rank(process_id,queue,ground_kb,path_rank_model_path,save_dir=""):
    if opt.ground_kb == "WIKIDATA":
        wikidata_init(DATASET[opt.dataset])
    elif opt.ground_kb == "FREEBASE":
        freebase_init(DATASET[opt.dataset])

    generator = TemporalQueryGenerator(ground_kb, path_rank_model_path)
    avg_max_f1 = 0.0

    total = 0
    while True:
        parsed_question = queue.get()
        if parsed_question is None:
            break
        print("question {}:{}".format(parsed_question.id,parsed_question.question))
        try:
            graphs = generator.generate(parsed_question)
            graphs_with_score = []
            for graph in graphs:
                predicts = graph.get_answers()
                if len(predicts) == 0:
                    continue
                evaluator = Evaluator(predicts,parsed_question.answers)
                graph.score = evaluator.f1
                graph_with_score = {"graph":graph,"p":evaluator.p,"r":evaluator.r,"f1":evaluator.f1,"hit1":evaluator.hit1}
                graphs_with_score.append(graph_with_score)
            graphs_with_score.sort(key=lambda g:g["f1"],reverse=True)
            rank_sample = {"question":parsed_question.question,"graphs_with_score":graphs_with_score}
            if len(graphs_with_score) > 0:
                print("max F1:{}".format(graphs_with_score[0]["f1"]))
                print("query graph:{}".format(graphs_with_score[0]["graph"].to_label_seq()))
                print("frame_name:{}".format(graphs_with_score[0]["graph"].frame_name))
                print("sparql:{}".format(graphs_with_score[0]["graph"].to_sparql()))
                avg_max_f1 += graphs_with_score[0]["f1"]
        except Exception as err:
            print(err)
            traceback.print_exc()
            write_cache()
            continue
        save_path = os.path.join(save_dir,parsed_question.id+".pkl")
        dump_pickle_dataset(rank_sample,save_path)
        total += 1
    if opt.ground_kb == "WIKIDATA":
        write_cache()
    elif opt.ground_kb == "FREEBASE":
        write_freebase_cache()
    if total > 0:
        avg_max_f1 /= total
    print("process{} avg max F1:{:.4f}".format(process_id,avg_max_f1))
    print("process{} finish".format(process_id))

def get_undo_samples(dataset,save_dir):
    rest_samples = []
    for sample in dataset:
        sample_save_path = os.path.join(save_dir,sample.id+".pkl")
        if not os.path.exists(sample_save_path):
            rest_samples.append(sample)
    return rest_samples

def parse_args():
    parser = OptionParser()
    parser.add_option("-t","--task",action='store',type='string',dest='task',default="graph_rank")
    parser.add_option("-i","--input_path",action='store',type='string',dest='input_path',default="/home2/hchen/TimeQA/data/dataset/TimeQuestions/cases.json")
    parser.add_option("-s","--save_dir",action="store",type="string",dest="save_dir",default="data/dataset/TimeQuestions/graph_rank/case_cache")
    parser.add_option("-d","--dataset",action='store',type='string',dest='dataset',default="TQ")
    parser.add_option("-g","--ground_kb",action="store",type="string",dest="ground_kb",default="WIKIDATA")
    parser.add_option("-w","--worker_num",action="store",type="int",dest="worker_num",default=1)
    parser.add_option("-m","--mode",action="store",type="string",dest="mode",default="start") #continue or start
    parser.add_option("--que_size",action="store",type="int",dest="que_size",default=100)
    parser.add_option("--path_rank_model_path",action="store",type="string",dest="path_rank_model_path",default="data/models/path_rank_tq_best_neg10_3090.pth")
    opt,args = parser.parse_args()
    return opt,args

if __name__ == "__main__":
    opt,_ = parse_args()
    
    dataset = load_parsed_questions(opt.input_path,DATASET[opt.dataset],opt.ground_kb)

    if opt.mode == "start" and os.path.exists(opt.save_dir):
        if os.path.isdir(opt.save_dir):
            shutil.rmtree(opt.save_dir)
        else:
            os.remove(opt.save_dir)

    if not os.path.exists(opt.save_dir):
        os.makedirs(opt.save_dir)
    
    if opt.mode == "continue":
        dataset = get_undo_samples(dataset,opt.save_dir)

    if len(dataset) == 0:
        print("finish")
    else:
        if opt.worker_num > len(dataset):
            opt.worker_num = len(dataset)

        processes = []
        queue = Queue(maxsize=opt.que_size)
        for i in range(opt.worker_num):
            if opt.task == "path_rank":
                process = Process(target=prepare_path_rank,args=(i,queue,opt.ground_kb,opt.save_dir))
            elif opt.task == "graph_rank":
                process = Process(target=prepare_graph_rank,args=(i,queue,opt.ground_kb,opt.path_rank_model_path,opt.save_dir))
            processes.append(process)
        
        for process in processes:
            process.start()

        for sample in dataset:
            queue.put(sample)
        
        for i in range(opt.worker_num):
            queue.put(None)
        
        for process in processes:
            process.join()
        
        print("all processes finish")