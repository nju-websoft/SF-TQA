class Evaluator():
    def __init__(self,predicts,answers):
        predicts = set(predicts)
        answers = set(answers)
        best_predict = None if len(predicts) == 0 else next(iter(predicts))
        self.p,self.r,self.f1,self.hit1 = self.cal_eval_metric(best_predict,predicts,answers)
    
    def cal_eval_metric(self,best_pred, preds, answers):
        correct, total = 0.0, 0.0
        for entity in preds:
            if entity in answers:
                correct += 1
            total += 1
        if len(answers) == 0:
            if total == 0:
                return 1.0, 1.0, 1.0, 1.0  # precision, recall, f1, hits
            else:
                return 0.0, 1.0, 0.0, 0.0  # precision, recall, f1, hits
        else:
            if total == 0:
                return 1.0, 0.0, 0.0, 0.0  # precision, recall, f1, hits
            else:
                hits = float(best_pred in answers)
                precision, recall = correct / total, correct / len(answers)
                f1 = 2.0 / (1.0 / precision + 1.0 / recall) if precision != 0 and recall != 0 else 0.0
                return precision, recall, f1, hits
