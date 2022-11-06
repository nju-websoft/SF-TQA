from utils.dataset_utils import load_json_dataset
from torch.utils.data import Dataset,DataLoader

class RankTrainDataset(Dataset):
    def __init__(self,file_path):
        data = load_json_dataset(file_path)
        self.dataset = []
        for sample in data:
            self.dataset.append((sample["question"],sample["positive"][0],1))
            for neg_text in sample["negatives"]:
                self.dataset.append((sample["question"],neg_text,0))
    
    def __getitem__(self,index):
        question = self.dataset[index][0]
        text = self.dataset[index][1]
        label = self.dataset[index][2]
        return question,text,label
    
    def __len__(self):
        return len(self.dataset)


class CEDataset(Dataset):
    def __init__(self, file_path):
        data = load_json_dataset(file_path)
        self.dataset = []
        for sample in data:
            question_label = sample["question"]
            new_sample = question_label + " [SEP] " + sample["positive"]
            for neg_graph in sample["negatives"]:
                new_sample += "[BAT]" + question_label + " [SEP] " + neg_graph
            self.dataset.append(new_sample)

    def __getitem__(self, index):
        return self.dataset[index]

    def __len__(self):
        return len(self.dataset)


class BCETrainDataset(Dataset):
    def __init__(self, file_path):
        data = load_json_dataset(file_path)
        self.dataset = []
        for sample in data:
            self.dataset.append((sample["question"], sample["graph_label"], sample["label"]))

    def __getitem__(self, index):
        question = self.dataset[index][0]
        text = self.dataset[index][1]
        label = self.dataset[index][2]
        return question, text, label

    def __len__(self):
        return len(self.dataset)