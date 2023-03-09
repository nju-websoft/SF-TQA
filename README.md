# SF-TQA

## Description

Code for `Semantic Framework based Query Generation for Temporal Question Answering over Knowledge Graphs`

https://aclanthology.org/2022.emnlp-main.122/

## Code

The code is structured as follows:

- evaluate
  - `evaluate.py` to evaluate predict results with answers.
- query
  - `querygraph.py` provides the structure and basic methods of *query graph*, including adding, deleting and changing nodes and edges on the graph, and provides serialization (for output and to sparql) methods.
  - `freebase_element.py` and `wikidata_element.py` provides the basic structure of SF-TCons Event, Property, TemporalEntity classes on two knowledge bases
  - `freebase_template.py` and `wikidata_template.py` provides each structure in SF-TCons on corresponding knowledge bases
- rank
  - `freebase_tag.py` and `wikidata_tag.py` provides methods for serializing query graphs on two knowledge bases, mainly for ranking
  - `multi_query_cache_prepare.py` Multi-threaded cache generation for path ranking and graph ranking
  - `graph_rank_train.py` and `path_rank_train.py` and the corresponding config files are for training path ranking and graph ranking models
  - `model.py` and `ranker.py` provide model details
- run
  - `annotate_time.py` adds a default base time to each problem in the dataset
  - `prepare_teq.py` and `prepare_tq.py` converts the annotations of datasets into a structured form
  - `cache_statistic.py` calculates the upper limit of the dataset when the best result is obtained for each problem in the cache
  - `check_cache_sparql.py` to check if there are any problems in the cache with skewed results due to instability of the query site
  - `evaluate_on_other_system_on_TEQ.py` to get Table 3 of the paper
  - `evaluate_metric.py` is used to calculate the dataset metrics according to the results of the real-time run
  - `evaluate_metric_by_cache.py` is run by running the prepared cache (because of the large number of datasets, the normal run one question at a time takes a huge amount of time, also affected by the query network, in addition to the uncertainty of hit1, so we run a version of the test set results cache with fixed results as the final generation of the system) (All the metrics are also calculated based on this cache) to get Table 3 and Table 4 of the run results in the paper
  - `evaluate_metric_by_cache_for_ablation.py` is used to get the results of the ablation experiments in the paper Table 5
- semantic
  - `semantic.py` provides the individual elements of the temporal element annotation
- solve
  - `solve.py` provides TemporalQuestionSolver class for generating query graphs, predicting results, serialization methods and recording results for each problem, with the solve method being the main method
  - `generate.py` records the annotation information and knowledge base of each question, and calls the query graph generator to generate query graphs.
  - `build.py` implements the query graph generation, determines whether each frame is available according to the annotation information of each problem, and calls the grounding method of each frame.
  - `frame.py` provides the existence conditions of each frame and the grounding method calls.
- In utils are the specific tools and methods of the two knowledge bases

## 中文说明

- evaluate
  - evaluate.py to evaluate predict results with answers.
- query
  - querygraph.py 提供query graph的结构和基本方法，包括图上节点和边的增删改，并且提供序列化（用于输出）和to sparql的方法
  - freebase_element.py or wikidata_element.py  提供两个知识库上SF-TCons的基础结构Event、Property、TemporalEntity类
  - freebase_template.py or wikidata_template.py提供两个知识库上SF-TCons里的各个结构
- rank
  - freebase_tag.py or wikidata_tag.py 提供两个知识库上query graph序列化的方法，主要用于排序
  - multi_query_cache_prepare.py 多线程生成路径排序和图排序的cache
  - graph_rank_train.py or path_rank_train.py两个文件和对应的config文件用于训练路径排序和图排序模型
  - model.py和ranker.py提供模型细节
- run
  - annotate_time.py给数据集的每个问题加上默认基准时间
  - prepare_teq.py or prepare_tq.py 将数据集的标注转换为有结构的形式
  - cache_statistic.py 用来计算cache里每个问题取最好结果时，数据集的上限
  - check_cache_sparql.py用来确认cache里有没有因为query网站不稳定而有结果偏差的问题
  - evaluate_on_other_system_on_TEQ.py用来得到论文中的Table 3
  - evaluate_metric.py用于按照实时运行的结果来计算数据集指标
  - evaluate_metric_by_cache.py是通过运行准备好的cache（因为数据集数量很大，正常一个问题一个问题地运行耗时巨大，也会受查询网络的影响，除此之外，hit1有不确定性，所以我们跑出了一版固定结果的测试集结果cache作为系统最后的生成，所有的指标也都是基于这个cache计算的）得到论文中的运行结果Table 3和Table 4
  - evaluate_metric_by_cache_for_ablation.py用来得到论文中的消融实验结果Table 5
- semantic
  - semantic.py提供时间元素标注的各个元素
- solve
  - solve.py 提供TemporalQuestionSolver类用来生成每个问题的query graph生成、预测结果、序列化方法和结果记录，slove方法是主要方法
  - generate.py记录每个问题的标注信息和知识库，调用query graph生成器来生成query graph
  - build.py 实现了生成query graph，根据每个问题的标注信息依次判断是否有各个frame，并且调用各个frame的grounding方法
  - frame.py提供了各个frame的存在条件和grounding方法调用
- utils里是两个知识库的具体工具方法

