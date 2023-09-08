import json
import pandas as pd
import re
import time
import warnings
from datetime import datetime
from pymilvus import connections, Collection, utility
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from utils.utils import get_object_size, transform_es_return_format, keyword_preprocess, sum_scores, process_results
from utils.sms.sequence_matcher_scoring import sequence_matcher_scoring
from utils.es_search import esQuery, get_final_result
from utils.milvus import connect_to_milvus, get_collection, search
from memory_profiler import profile

warnings.simplefilter("ignore") # TODO: remove

es = Elasticsearch(
    hosts="trueint.lu.im.ntu.edu.tw",
    port=9200,
    timeout=180
    # 'http://localhost:9200/'  # 連線叢集，以列表的形式存放各節點的IP地址
    # sniff_on_start=True,    # 連線前測試
    # sniff_on_connection_fail=True,  # 節點無響應時重新整理節點
    # sniff_timeout=60,    # 設定超時時間
    # ignore=400,  # 忽略返回的400狀態碼
    # ignore=[400, 405, 502],  # 以列表的形式忽略多個狀態碼
    # http_auth=('elastic', 'changeme')  # 認證資訊
)

# milvus TODO: only need to load once
connect_to_milvus()
# collection = get_collection('pinyin_embedding_300')
# collection.release()
# collection = get_collection('pinyin_embedding_300_L2')
ll = utility.list_collections()
for c in ll:
    collection = Collection(c)
    print("collection", c)
    collection.release()

print("collection release done")
collection = get_collection('glyph_embedding_3619_no_length')
# collection = get_collection('pinyin_embedding_300_L2')
print("get collection done")

df = pd.read_csv("/home/ericaaaaaaa/logoshot/utils/vector/glyph/CNS_SUMMARY_TABLE.csv", encoding="utf8")

# @profile
def text_search(
    glyph=False,
    pinyin=False,
    target_tmNames="",
    target_draft_c="",
    target_draft_e="",
    target_draft_j="",
    target_classcodes=[],
    target_color="",
    target_applicant="",
    target_startTime="",
    target_endTime="",
    es=es,
    correct_ans="", #TODO: will be removed
):
    """
    Execute the complete process of text searching. The process includes:
    - compute the score of each trademark according to the criteria
    - sum the score of each trademark obtained from different data source,
      such as milvus, elastic search and sequence_matcher_scoring
    - sort trademarks based on the score
    - return the trademarks with the highest sum_scores
    
    ===
    input
    - target_tmNames, target_draft_c, ...: search criterias targeted by users
    - glyph: whether the searching needs to consider glyph similarity
    - pinyin: whether the searching needs to consider sound similarity
    - es: elastic search
    output
    - final_result: a list which consists of all trademark documents which
      matches the criteria
    """
    milvus_threshold = -10   # the threshold to decide whether we only need to run milvus
    str_mode = "音"
    if glyph:
        milvus_threshold = 0.7
        str_mode = "形"
    sms_threshold = 0.7
    es_return_size = 1000
    sms_return_size = 1000
    milvus_key_index = 0   # if score of that index > threshold, there's no need for further searching
    data_shown = 10   # also top #
    target_id_list = []
    results_id_list = []
    results = []
    milvus_results = []
    es_results = []
    sms_results = []
    same_length_results = []
    different_length_results = []
    final_results = []
    weight = 1
    nprobe = 1000
    # variables for testing TODO: will be removed
    map_tmName = True
    in_top = 99999
    caseType = ""

    # preprocess
    target_tmNames = keyword_preprocess(target_tmNames) 
    
    st = time.time()

    # strict search
    if target_tmNames != "" and glyph == False and pinyin == False:
        print("mode: 嚴格搜尋")
        es_results = esQuery(
            es=es,
            mode="strict",
            target_tmNames=target_tmNames,
            target_draft_c=target_draft_c,
            target_draft_e=target_draft_e,
            target_draft_j=target_draft_j,
            target_classcodes=target_classcodes,
            target_color=target_color,
            target_applicant=target_applicant,
            target_startTime=target_startTime,
            target_endTime=target_endTime,
            return_size=es_return_size,
        )
        results = es_results
        map_tmName = False
        caseType = "嚴格搜尋"
    # 形近 / 音近搜尋
    else:
        # milvus query
        if pinyin:
            milvus_results = search(nprobe=nprobe, target=target_tmNames, collection=collection, type="L2")
        else:
            milvus_results = search(nprobe=nprobe, target=target_tmNames, collection=collection, type="IP")
        print("milvus_results", milvus_results[:data_shown])
        milvus_time = time.time() - st
        print("---milvus done", milvus_time)
        
        # check if we need to search through other data sources (when there are
        # other search criteria, or when the top score of milvus results is low)
        if milvus_results[milvus_key_index][1] > milvus_threshold:
            if (
            target_draft_c != ""
            or target_draft_e != ""
            or target_draft_j != ""
            or target_classcodes != []
            or target_color != ""
            or target_applicant != ""
            or target_startTime != ""
            or target_endTime != ""
            ):
                print(f"mode: {str_mode}近搜尋、有其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
                target_id_list = [appl_no for appl_no, score in milvus_results]
                
                # elastic search by id list from milvus and other search criteria
                es_results = esQuery(
                    es=es,
                    mode="same",
                    target_id_list=target_id_list,
                    # target_tmNames=target_tmNames,
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                )
                print("es results", es_results[:data_shown])
                es_time = time.time() - milvus_time - st
                print("---es done", es_time)
                
                # sum es score and milvus score
                results = sum_scores(milvus_results, es_results, False)
                print("summed results", results[:data_shown])
                sum_time = time.time() - es_time - milvus_time - st
                print("---sum score done", sum_time)

                caseType = "milvus (> threshold) + es (other filter)"
            else:
                print(f"mode: {str_mode}近搜尋、無其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
                results = milvus_results     
                caseType = "milvus (> threshold)"
        else:
            if (
                target_draft_c != ""
                or target_draft_e != ""
                or target_draft_j != ""
                or target_classcodes != []
                or target_color != ""
                or target_applicant != ""
                or target_startTime != ""
                or target_endTime != ""
            ):
                print(f"mode: {str_mode}近搜尋、有其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
                # deal with trademarks of the same length as the keyword
                # elastic search by other search criteria
                target_id_list = list(map(lambda x: x[0], milvus_results))
                es_results = esQuery(
                    es=es,
                    mode="same",
                    target_id_list=target_id_list,
                    # target_tmNames=target_tmNames,
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                )
                print("es_results", es_results[:data_shown])
                es_time_same = time.time() - milvus_time - st
                print("---es done", es_time_same)
                
                # sum es score and milvus score
                same_length_results = sum_scores(milvus_results, es_results, False)
                print("same_length_results", same_length_results[:data_shown])
                sum_time_same = time.time() - es_time_same - milvus_time - st
                print("---sum score done", sum_time_same)

                # deal with trademarks of different length from the keyword
                # elastic search by other search criteria
                es_results = esQuery(
                    es=es,
                    mode="different_score",
                    # target_tmNames=target_tmNames,
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                    length=len(target_tmNames),
                )
                print("es_results", es_results[:data_shown])
                es_time_diff = time.time() - es_time_same - sum_time_same - milvus_time - st
                print("---es done", es_time_diff)

                # get the id and trademark
                es_results_id = [appl_no for appl_no, tmName, score in es_results]
                es_results_tmName = [tmName for appl_no, tmName, score in es_results]
                
                # sequence matcher scoring
                sms_results = sequence_matcher_scoring(es_results_id, es_results_tmName, target_tmNames, sms_threshold, glyph)
                sms_time = time.time() - es_time_diff - es_time_same - sum_time_same - milvus_time - st
                print("---sms done", sms_time)

                # weight milvus results        
                if not glyph:        
                    # weight = (-1) * milvus_results[0][1] / sms_results[0][1]
                    weight = milvus_results[0][1] / (1 - sms_results[0][1]) #TODO: there's bug if sms = 1
                    sms_results = [(appl_no, (1 - score) * weight) for appl_no, score in sms_results]
                else:
                    sms_results = [(appl_no, score * weight) for appl_no, score in sms_results]

                # sum es score and sms score
                different_length_results = sum_scores(sms_results, es_results, False)
                print("different_length_results", different_length_results[:data_shown])
                sum_time_diff = time.time() - sms_time - es_time_diff - es_time_same - sum_time_same - milvus_time - st
                print("---sum score done", sum_time_diff)

                # combine results of trademarks of same and different length
                results.extend(same_length_results)
                results.extend(different_length_results)
                caseType = "milvus (< threshold) + es (other filter) + sms"
            else:
                print(f"mode: {str_mode}近搜尋、無其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
                # elastic search to get trademarks of different length from the keyword
                es_results = esQuery(
                    es=es,
                    mode="different",
                    target_tmNames=target_tmNames,
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                )
                print("es_results", es_results[:data_shown])
                print("es length", len(es_results))
                es_time = time.time() - milvus_time - st
                print("---es done", es_time)

                # get the id and trademark name TODO
                # es_results_id = [appl_no for appl_no, tmName, score in es_results]
                # es_results_tmName = [tmName for appl_no, tmName, score in es_results]
                es_results_id = [appl_no for appl_no, tmName in es_results]
                es_results_tmName = [tmName for appl_no, tmName in es_results]
                
                # sequence matcher scoring
                sms_results = sequence_matcher_scoring(es_results_id, es_results_tmName, target_tmNames, sms_threshold, glyph)
                print("sms_results", sms_results[:data_shown])
                sms_time = time.time() - es_time - milvus_time - st
                print("---sms done", sms_time)

                # weight milvus results
                if not glyph:
                    # weight = (-1) * milvus_results[0][1] / sms_results[0][1] TODO
                    weight = milvus_results[0][1] / (1 - sms_results[0][1])
                # sms_results = [(appl_no, score * weight) for appl_no, score in sms_results]
                    sms_results = [(appl_no, (1 - score) * weight) for appl_no, score in sms_results]
                else:
                    sms_results = [(appl_no, score * weight) for appl_no, score in sms_results]

                # combine results of trademarks of same and different length
                results.extend(milvus_results)
                results.extend(sms_results[:sms_return_size])
                caseType = "milvus (< threshold) + es (only query) + sms"      
    
    # sort results by score
    results = process_results(results)
    print("total results:", len(results), "records")
    print("final results", results[:data_shown])

    et = time.time()
    print("text_search time used", et - st)
    
    # check results (TODO: will be removed)
    if map_tmName:
        results_id_list = [appl_no for appl_no, tmName in results[:data_shown]] #TODO here
        tmp = esQuery(
            es=es,
            mode="same",
            target_id_list=results_id_list,
            return_size=data_shown * 3,
        )
        check_results = sum_scores(results, tmp, True)
        print("check results", check_results[:data_shown])
        check_results = sorted(check_results, key= lambda x: x[-1], reverse=True)
        print("map_tmName results")
        print(check_results[:data_shown])

        # check if target in top hits
        for r in check_results[:data_shown]:
            if correct_ans == r[1]:
                print("correct!")
                if r[0] in results_id_list:
                    in_top = results_id_list.index(r[0]) + 1
                    break
    else:
        print("!map_tmName results")
        print(results[:data_shown])
        
        results_id_list = [appl_no for appl_no, tmName, score in results[:data_shown]] #TODO here
        # check if target in top ten
        for r in results[:data_shown]:
            if correct_ans == r[1]:
                print("correct!")
                print("result id", results_id_list[:5])
                if r[0] in results_id_list:
                    in_top = results_id_list.index(r[0]) + 1
                    break
    
    # query full document of each result
    if results_id_list != []:
        final_results = get_final_result(es, results_id_list)
    else:
        final_results = results

    return final_results, in_top, caseType, et - st # some outputs just for tests TODO