import json
from optparse import OptionParser
from utils.sutime_utils import annotate_datetime
def parse_args():
    parser = OptionParser()
    parser.add_option('-r',dest="ref_date",type=str,default="2020-04-01",help="reference date")
    parser.add_option('-d',dest="dataset_path",type=str,help="dataset path")
    parser.add_option('-o',dest="output_path",type=str,help="output path")
    opt,args = parser.parse_args()
    return opt,args

def read_dataset(path):
    with open(path,"rt",encoding="utf-8") as fin:
        dataset = json.load(fin)
        return dataset

def write_dataset(dataset,path):
    with open(path,"wt",encoding="utf-8") as fout:
        json.dump(dataset,fout)

if __name__ == "__main__":
    opt,args = parse_args()
    dataset = read_dataset(opt.dataset_path)
    for sample in dataset:
        question = sample["Question"]
        time_annotates = annotate_datetime(question,opt.ref_date)
        sample["time_annnotates"] = time_annotates
    write_dataset(dataset,opt.output_path)
    
