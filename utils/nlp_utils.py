import re
import spacy

first_words = ["first","start","youngest","earliest","primary","origin"]
last_words = ["last","end","finish","final","finally","oldest"]
ordinal_dict= {"first":1,"second":2,"third":3,"fourth":4,"fifth":5,"sixth":6,"seventh":7,"eighth":8,\
    "ninth":9,"tenth":10,"eleventh":11,"twelfth":12,"thirteenth":13,"fourteenth":14,"fifteenth":15,"sixteenth":16,\
    "seventeenth":17,"eighteenth":18,"nineteenth":19,"twentieth":20,"thirty-first":31,"fortieth":40,"fiftieth":50,\
        "sixtieth":60,"seventieth":70,"eightieth":80,"ninetieth":90}
num_regex = re.compile(r"\d+")
nlp = spacy.load("en_core_web_lg")

def parse_ordinal(order_word):
    order_words = order_word.lower().split()
    for word in order_words:
        if word in first_words:
            return 1
        elif word in last_words:
            return -1
        elif word in ordinal_dict.keys():
            return ordinal_dict[word]
        num = num_regex.search(word)
        if num is not None:
            return int(num.group())

def spacy_text_similarity(text1,text2):
    if(len(text1)==0 or len(text2)==0):
        return 0
    doc1 = nlp(text1)
    doc2 = nlp(text2)
    score = doc1.similarity(doc2)
    return score

if __name__ == "__main__":
    score = spacy_text_similarity("I like apples","I like oranges")
    print(score)