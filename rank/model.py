import torch.nn as nn
import torch
from transformers import BertModel


class BertScorer(nn.Module):
    def __init__(self, bert_model):
        super(BertScorer, self).__init__()
        self.encoder = bert_model
        self.linear = nn.Linear(768, 1)
        self.loss = nn.BCEWithLogitsLoss()
        self.ce_loss = nn.CrossEntropyLoss()
        self.bce_loss = nn.BCEWithLogitsLoss()

    def forward(self, X):
        bert_output = self.encoder(**X)
        cls = bert_output.last_hidden_state[:, 0, :]
        scores = self.linear(cls)
        scores = scores.reshape(-1)
        return scores

    def get_loss(self, X, y):
        scores = self.forward(X)
        loss = self.loss(scores, y.float())
        return loss

    def get_ce_loss(self, X, bz):
        scores = self.forward(X)
        scores = scores.reshape(bz, -1)
        y = torch.zeros(bz, device=scores.device)
        loss = self.ce_loss(scores, y.long())
        return loss

    def get_bce_loss(self, X, y):
        scores = self.forward(X)
        loss = self.bce_loss(scores, y.float())
        return loss

    def get_combine_loss(self, X, bz):
        scores1 = self.forward(X)
        scores2 = scores1.reshape(bz, -1)
        rank_num = scores2.size()[1]
        y1 = [1] + [0] * (rank_num - 1)
        y1 = torch.tensor(y1 * bz, device=scores1.device)
        y2 = torch.zeros(bz, device=scores2.device)
        a = 1
        loss = a * self.bce_loss(scores1, y1.float()) + (1 - a) * self.ce_loss(scores2, y2.long())
        return loss

    @staticmethod
    def from_pretrained(*args, **kargs):
        bert = BertModel.from_pretrained(*args, **kargs)
        model = BertScorer(bert)
        return model

    @staticmethod
    def from_trained(path):
        model = torch.load(path, map_location=torch.device('cpu'))
        model.ce_loss = nn.CrossEntropyLoss()
        return model
