from rank.dataset import CEDataset
from rank.model import BertScorer
from utils.dataset_utils import *
from transformers import BertTokenizerFast, AdamW, get_linear_schedule_with_warmup
from torch.utils.data import DataLoader
import torch
import torch.nn as nn
import os
import numpy as np
from optparse import OptionParser

def parse_args():
    parser = OptionParser()
    parser.add_option("-c","--config_file",action='store',type='string',dest='config_path',default="rank/graph_rank_train_config.json")
    parser.add_option("-s", "--seed", action='store', type='int', dest='seed',default=42)
    parser.add_option("--best_model_save_path", action='store', type='string', dest='model_save_path')
    opt,args = parser.parse_args()
    return opt,args

class Trainer():
    def __init__(self, train_config_path="rank/graph_rank_train_config.json"):
        self.train_config = load_json_dataset(train_config_path)

    def train(self):
        if self.train_config["device"] == "cpu":
            device = torch.device("cpu")
        else:
            os.environ['CUDA_VISIBLE_DEVICES'] = self.train_config["gpus"]
            device = torch.device("cuda:0")
        batch_size = self.train_config["batch_size"]

        added_tokens = ['[ANS]', '[VAR0]', '[VAR1]', '[VAR2]', '[VAR3]', '[VAR4]', '[T1]', '[T2]', '[SIG]', '[TAR]',
                        '[REL]']
        tokenizer = BertTokenizerFast.from_pretrained(self.train_config["pretrained_model_path"],
                                                      additional_special_tokens=added_tokens)
        # tokenizer = BertTokenizerFast.from_pretrained(self.train_config["pretrained_model_path"])
        if self.train_config["load_model_path"] is not None:
            model = BertScorer.from_trained(self.train_config["load_model_path"])
        else:
            model = BertScorer.from_pretrained(self.train_config["pretrained_model_path"])
        if self.train_config["device"] == "gpu":
            model = nn.DataParallel(model)
            model.to(device)
            model = model.module

        train_dataset = CEDataset(self.train_config["train_dataset_path"])
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

        lr = self.train_config["lr"]
        eps = self.train_config["eps"]
        epochs = self.train_config["epochs"]
        optimizer = AdamW(model.parameters(), lr=lr, eps=eps)
        total_steps = len(train_loader) * epochs
        scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=0, num_training_steps=total_steps)

        step = 0
        save_steps = self.train_config["save_steps"]
        model_path = self.train_config["save_path"]
        best_model_path = self.train_config["best_model_path"]
        best_dev_acc = 0.0

        for i in range(epochs):
            train_loss = 0.0
            for batch in train_loader:
                bz = len(batch)
                flat_batch = []
                for sample in batch:
                    flat_batch += sample.split("[BAT]")
                inputs = tokenizer(flat_batch, padding=True, return_tensors="pt")
                inputs = {key: value.to(device) for key, value in inputs.items()}
                model.zero_grad()
                loss = model.get_ce_loss(inputs, bz)
                # loss = model.get_combine_loss(inputs,bz)

                train_loss += loss.item() * bz

                loss.backward()
                optimizer.step()
                scheduler.step()

                step += 1

                if step % save_steps == save_steps - 1:
                    torch.save(model, model_path)
            print("finish epoch {}".format(i + 1))
            print("avg training loss:{:.4f}".format(train_loss / len(train_dataset)))
            dev_acc = self.eval_acc(tokenizer, model, device)
            print("dev acc:{:.4f}".format(dev_acc))
            if dev_acc >= best_dev_acc:
                best_dev_acc = dev_acc
                print("best dev acc:{:.4f}".format(dev_acc))
                print("save best model to path {}".format(best_model_path))
                torch.save(model, best_model_path)
            else:
                print("best dev acc:{:.4f}".format(best_dev_acc))
            print()

    def eval_acc(self, tokenizer, model, device):
        batch_size = self.train_config["batch_size"]
        dev_dataset = CEDataset(self.train_config["dev_dataset_path"])
        dev_loader = DataLoader(dev_dataset, batch_size=batch_size, shuffle=True)
        hit = 0
        for batch in dev_loader:
            bz = len(batch)
            flat_batch = []
            for sample in batch:
                flat_batch += sample.split("[BAT]")
            inputs = tokenizer(flat_batch, padding=True, return_tensors="pt")
            inputs = {key: value.to(device) for key, value in inputs.items()}
            with torch.no_grad():
                scores = model.forward(inputs)
                scores = scores.reshape(bz, -1)
                predicts = scores.argmax(axis=1)
                labels = torch.zeros(bz, device=predicts.device)
                hit += torch.eq(predicts, labels).sum().item()
        dev_acc = hit / len(dev_dataset)
        return dev_acc


def set_seed(seed):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True


if __name__ == "__main__":
    opt, _ = parse_args()
    set_seed(opt.seed)
    trainer = Trainer(train_config_path=opt.config_path)
    trainer.train()




