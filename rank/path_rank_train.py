from rank.dataset import BCETrainDataset
from rank.model import BertScorer
from utils.dataset_utils import *
from transformers import BertTokenizerFast,AdamW,get_linear_schedule_with_warmup
from torch.utils.data import DataLoader
import torch
import torch.nn as nn
import os
import numpy as np

class Trainer():
    def __init__(self,train_config_path="rank/path_rank_train_config.json"):
        self.train_config = load_json_dataset(train_config_path)
    
    def train(self):
        if self.train_config["device"] == "cpu":
            device = torch.device("cpu")
        else:
            os.environ['CUDA_VISIBLE_DEVICES'] = self.train_config["gpus"]
            device = torch.device("cuda:0")
        batch_size = self.train_config["batch_size"]

        added_tokens = ['[ANS]','[VAR0]','[VAR1]','[VAR2]','[VAR3]','[VAR4]','[T1]','[T2]','[SIG]','[TAR]','[REL]']
        tokenizer = BertTokenizerFast.from_pretrained(self.train_config["pretrained_model_path"],additional_special_tokens=added_tokens)
        if self.train_config["load_model_path"] is not None:
            model = BertScorer.from_trained(self.train_config["load_model_path"])
        else:
            model = BertScorer.from_pretrained(self.train_config["pretrained_model_path"])
        if self.train_config["device"] == "gpu":
            model = nn.DataParallel(model)
            model.to(device)
            model = model.module

        train_dataset = BCETrainDataset(self.train_config["train_dataset_path"])
        train_loader = DataLoader(train_dataset,batch_size=batch_size,shuffle=True)

        lr = self.train_config["lr"]
        eps = self.train_config["eps"]
        epochs = self.train_config["epochs"]
        optimizer = AdamW(model.parameters(),lr=lr,eps=eps)
        total_steps = len(train_loader) * epochs
        scheduler = get_linear_schedule_with_warmup(optimizer,num_warmup_steps=0,num_training_steps=total_steps)

        step = 0
        save_steps = self.train_config["save_steps"]
        model_path = self.train_config["save_path"]
        best_model_path = self.train_config["best_model_path"]
        best_dev_loss = 1e10

        for i in range(epochs):
            train_loss = 0.0
            for batch in train_loader:
                questions = batch[0]
                graph = batch[1]
                y = batch[2].to(device)
                inputs = tokenizer(questions,graph,padding=True,return_tensors="pt")
                inputs = {key:value.to(device) for key,value in inputs.items()}
                model.zero_grad()
                loss = model.get_bce_loss(inputs,y)

                train_loss += loss.item() * len(batch[0])

                loss.backward()
                optimizer.step()
                scheduler.step()

                step += 1

                if step % save_steps == save_steps - 1:
                    torch.save(model,model_path) 
            print("finish epoch {}".format(i+1))
            print("avg training loss:{:.4f}".format(train_loss/len(train_dataset)))
            dev_loss = self.eval(tokenizer,model,device)
            print("dev loss:{:.4f}".format(dev_loss))
            if dev_loss < best_dev_loss:
                best_dev_loss = dev_loss
                print("best dev loss:{:.4f}".format(dev_loss))
                print("save best model to path {}".format(best_model_path))
                torch.save(model,best_model_path)
            else:
                print("best dev loss:{:.4f}".format(best_dev_loss))
            print()
    
    def eval(self,tokenizer,model,device):
        batch_size = self.train_config["batch_size"]
        dev_dataset = BCETrainDataset(self.train_config["dev_dataset_path"])
        dev_loader = DataLoader(dev_dataset,batch_size=batch_size,shuffle=True)
        dev_loss = 0.0
        for batch in dev_loader:
            questions = batch[0]
            graph = batch[1]
            y = batch[2].to(device)
            inputs = tokenizer(questions,graph,padding=True,return_tensors="pt")
            inputs = {key:value.to(device) for key,value in inputs.items()}
            with torch.no_grad():
                loss = model.get_bce_loss(inputs,y)
            dev_loss += loss.item() * len(batch[0])
        dev_loss = dev_loss / len(dev_dataset)
        return dev_loss

def set_seed(seed=42):
    random.seed(seed)
    os.environ['PYTHONHASHSEED']=str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)  # if you are using multi-GPU.
    torch.manual_seed(seed)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True

if __name__ == "__main__":
    set_seed(42)
    trainer = Trainer()
    trainer.train()
