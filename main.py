from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json
import pandas as pd
import re
import time
from datetime import datetime
from utils.utils import get_object_size, transform_es_return_format
from utils.sequence_matcher_scoring import sequence_matcher_scoring
from utils.es_search import esQuery
from utils.milvus import connect_to_milvus, get_collection, search
import warnings
from pymilvus import connections, Collection

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
# 下面兩行只有一開始要跑一次 TODO: here?
connect_to_milvus()
collection = get_collection('pinyin_embedding_300')

df = pd.read_csv("/home/ericaaaaaaa/logoshot/data/CNS_SUMMARY_TABLE.csv", encoding="utf8")

def keyword_preprocess(searchKeywords):
    """
    將關鍵詞中的多個空格替代為一個空格，並將字串中由空格隔開的詞拆開，組成 list。
    """
    keyword = re.sub(" +", " ", searchKeywords).strip()
    keywords_list = keyword.split()

    return keywords_list

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
):
    """
    執行文字搜尋的完整流程，輸入各式條件後，最終將依據分數排序，回傳符合條件的結果，。

    ===
    input
    - 關鍵詞: searchKeywords, target_draft_c,...
    - 是否選擇音近字/形近字搜尋: isSimSound, isSimShape
    - es
    output
    - final_result: list, 儲存符合條件的商標的 document
    """
    milvus_threshold = 0.9   # the threshold to decide whether we only need to run milvus
    sms_threshold = 0.5
    es_return_size = 5000
    sms_return_size = 1000
    milvus_key_index = 0   # if score of that index > threshold, there's no need for further searching
    data_shown = 10
    target_id_list = []
    results_id_list = []
    results = []
    milvus_results = []
    es_results = []
    sms_results = []
    same_length_results = []
    different_length_results = []
    final_results = []
    map_tmName = True
    # variables for testing
    in_top_ten = False
    caseType = ""

    # preprocess
    target_tmName_list = keyword_preprocess(target_tmNames)
    target_tmName = target_tmName_list[0]
    
    st = time.time()

    # 嚴格搜尋
    if target_tmNames != "" and glyph == False and pinyin == False:
        print("mode: 嚴格搜尋")
        es_results = esQuery(
            es=es,
            mode="strict",
            target_id_list=target_id_list,
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
            length=0,
        )
        results = es_results
        map_tmName = False
    # 形近 / 音近搜尋
    else:
        # milvus query
        milvus_results = search(nprobe=100, target=target_tmName, collection=collection)
        print("milvus_results", milvus_results[:5])

        # 判斷是否需要進行更多搜尋 
        if milvus_results[milvus_key_index][1] > milvus_threshold:
            caseType = "只進 milvus"
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
                print("mode: 音近搜尋、有其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
                # filter ids with score > milvus_threshold
                target_id_list = list(map(lambda x: x[0], filter(lambda x: x[1] > milvus_threshold, milvus_results)))
                # print("target_id_list", target_id_list[:data_shown])
                
                # 用 id list 和其他條件進行 es search
                es_results = esQuery(
                    es=es,
                    mode="same",
                    target_id_list=target_id_list,
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
                    length=0,
                )
                results = es_results
                map_tmName = False
            else:
                print("mode: 音近搜尋、無其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
                results = milvus_results     
                caseType = 2    
        else:
            caseType = "有進 es search"
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
                print("mode: 音近搜尋、有其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
                # 針對長度相同的商標名稱
                # 取得這些商標名稱在其他條件中的 es search 得分
                target_id_list = list(map(lambda x: x[0], milvus_results))
                # print("target_id_list", target_id_list[:data_shown])
                es_results = esQuery(
                    es=es,
                    mode="same",
                    target_id_list=target_id_list,
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
                    length=0,
                )
                print("es_results", es_results[:data_shown])
                
                # 加總 es score 和 milvus score
                same_length_results = [
                    (appl_no, milvus_score + es_score)
                    for (appl_no, milvus_score), (appl_no, tmName, es_score) in zip(milvus_results, es_results)
                ]
                print("same_length_results", same_length_results[:data_shown])
                print()

                # 針對長度不同的商標名稱
                # 進行其他條件的 es search，取得前 5000 筆
                es_results = esQuery(
                    es=es,
                    mode="different_score",
                    target_id_list=target_id_list,
                    target_tmNames="",
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                    length=len(target_tmName),
                )
                print("es_results", es_results[:data_shown])

                # 只取出 id 和商標名稱
                es_results_id = [appl_no for appl_no, tmName, score in es_results]
                es_results_tmName = [tmName for appl_no, tmName, score in es_results]
                
                # 對前 5000 筆資料進行 sequence_matcher_scoring()
                sms_results = sequence_matcher_scoring(es_results_id, es_results_tmName, target_tmName, sms_threshold, glyph)
                print("sms_results", sms_results[:data_shown])

                # 加總 es score 和 sms score
                different_length_results = [
                    (appl_no, sms_score + es_score)
                    for (appl_no, sms_score), (appl_no, tmName, es_score) in zip(sms_results, es_results)
                ]
                print("different_length_results", same_length_results)

                # 合併長度相同與長度不同的結果，並排序
                results.extend(same_length_results)
                results.extend(different_length_results)
                results = sorted(results, key= lambda x: x[1], reverse=True)
            else:
                print("mode: 音近搜尋、無其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
                # 進行 es search，取得所有長度不同的商標之 id、商標名稱
                es_results = esQuery(
                    es=es,
                    mode="different",
                    target_id_list=target_id_list,
                    target_tmNames="",
                    target_draft_c=target_draft_c,
                    target_draft_e=target_draft_e,
                    target_draft_j=target_draft_j,
                    target_classcodes=target_classcodes,
                    target_color=target_color,
                    target_applicant=target_applicant,
                    target_startTime=target_startTime,
                    target_endTime=target_endTime,
                    return_size=es_return_size,
                    length=len(target_tmName),
                )
                print("es_results", es_results[:data_shown])

                # 只取出 id 和商標名稱
                partial = 500000
                es_results_id = [appl_no for appl_no, tmName in es_results[:partial]]
                es_results_tmName = [tmName for appl_no, tmName in es_results[:partial]]
                
                # 對所有資料進行 sequence_matcher_scoring()
                sms_st = time.time()
                sms_results = sequence_matcher_scoring(es_results_id, es_results_tmName, target_tmName, sms_threshold, glyph)
                print("sms_results", sms_results[:data_shown])
                sms_et = time.time()
                print("sms time used", et - st)

                # 合併長度相同與長度不同的結果，並排序
                results.extend(milvus_results)
                results.extend(sms_results[:sms_return_size])
    
    # sort results by score
    results = sorted(results, key= lambda x: x[-1], reverse=True)
    print("total results:", len(results), "records")

    et = time.time()
    print("text_search time used", et - st)
    
    # check results (TODO: will be removed)
    if map_tmName:
        results_id_list = [appl_no for appl_no, tmName in results]
        check_results = esQuery(
            es=es,
            mode="same",
            target_id_list=results_id_list,
            return_size=es_return_size,
        )
        print("results", results[:2])
        print("check results", check_results[:2])
        check_results = [
            (appl_no, tmName, score1)
            for (appl_no, score1), (appl_no, tmName, score2) in zip(results, check_results)
        ]
        print("results")
        print(check_results[:data_shown])

        # check if target in top ten
        for r in check_results:
            if target_tmName == r[1]:
                in_top_ten = True
                print(r)
                break
    else:
        print("results")
        print(results[:data_shown])
        
        # check if target in top ten
        for r in results:
            if target_tmName == r[1]:
                in_top_ten = True
                print(r)
                break
    
    # query full document of each result
    if results_id_list != []:
        final_results = es.mget(index="logoshot2022", body={"ids": results_id_list})["docs"]

    return final_results, in_top_ten, caseType, et - st # some outputs just for tests TODO