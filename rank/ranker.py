from transformers import BertTokenizerFast
import torch

from rank.model import BertScorer

class BertRanker():
    def __init__(self,model_path,batch_size=32,device=torch.device("cpu")):
        added_tokens = ['[ANS]','[VAR0]','[VAR1]','[VAR2]','[VAR3]','[VAR4]','[T1]','[T2]','[SIG]','[TAR]','[REL]']
        self.tokenizer = BertTokenizerFast.from_pretrained("/home2/hyli/projects/TemporalKBQA/data/pretrained_models/bert-base-uncased",additional_special_tokens=added_tokens)
        self.model = BertScorer.from_trained(model_path)
        self.device = device
        self.model.to(device)
        self.batch_size = batch_size

    def rank(self,question,labels):
        steps = (len(labels) - 1) // self.batch_size + 1
        scores = []
        for i in range(0,steps):
            start = i * self.batch_size
            end = min(start+self.batch_size,len(labels))
            label_batch = labels[start:end]
            question_batch = [question] * (end-start)
            try:
                inputs = self.tokenizer(question_batch,label_batch,padding=True,return_tensors="pt")
            except:
                print("l")
            inputs = {k:v.to(self.device) for k,v in inputs.items()}
            with torch.no_grad():
                output = self.model(inputs)
                scores += output.cpu().numpy().tolist()
        return scores
