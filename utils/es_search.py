from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch import helpers
import json
import pandas as pd
import re
import time
from datetime import datetime
import sys

es = Elasticsearch(
    hosts="trueint.lu.im.ntu.edu.tw",
    port=9200,
    timeout=180
)

def get_final_result(es, target_id_list):
    query_body = {"query": {"bool": {"filter":{"terms": {"appl-no": target_id_list}}}}}        
    result_detail = es.search(query_body, index="logoshot2022")
    # final_result = [[item['_source']['appl-no'], item['_source']['tmark-name'], item['_source']['tmark-image-url_1']] for item in result_detail['hits']['hits']]
    final_result = [item['_source'] for item in result_detail['hits']['hits']]
    
    return final_result

# ES 的搜索是有數量限制的，因此利用官方提供的滾動 API 實現了一個對全量數據處理的功能
# 應該是這邊跑太慢了 應該可以試著處理query的寫法 這裡回傳的resultAAA已經依照score排序
def travel_es(es, result_list, return_size, **kwargs):
    """
    遍歷es的搜索結果，並使用process_func處理返回的item
    process_func: function to process item.
    kwargs: arguments same as elasticsearch search api.
    """
    kwargs.setdefault("scroll", "2m")
    kwargs.setdefault("size", 1000)
    #res的結果已經按找score排序了
    res = es.search(**kwargs)

    sid = res['_scroll_id']
#     print("sid:", sid)

    scroll_size = len(res['hits']['hits'])

    total_size = scroll_size
    
    ## 最外面一層是一次scroll回傳的數量 這裡是10000 所以result_list有10層 每1層裡面1000筆 可以縮小res['hits']['hits']append的內容 主要所需是sourece跟id
    result = [[item['_source']['appl-no'], item['_source']['tmark-name'], item['_score']] for item in res['hits']['hits']]
    result_list.extend(result)
    # 不要讓程式搜尋太多不必要的結果
    if total_size <= return_size:
        while scroll_size > 0:
            "Scrolling..."

            # Before scroll, process current batch of hits
    #         process_func(res['hits']['hits'])

            data = es.scroll(scroll_id=sid, scroll='4m')

            # Update the scroll ID
            sid = data['_scroll_id']
    #         print("sid:", sid)

            # Get the number of results that returned in the last scroll
            # hits hits的東西很多 不見得要都回傳
            result = [[item['_source']['appl-no'], item['_source']['tmark-name'], item['_score']] for item in data['hits']['hits']]
            result_list.extend(result)

            scroll_size = len(result)
    #         print("scroll_size:", scroll_size)

            total_size += scroll_size

            # 不要讓程式搜尋太多不必要的結果
            if total_size >= return_size:
                break

    return total_size

def esQuery(mode, length = 0, target_tmNames="", target_id_list = [], return_size = 1000, isImageSearchFilter=False, target_draft_c="", target_draft_e="", target_draft_j="", target_classcodes=[], target_color="", target_applicant="", target_startTime="", target_endTime="", es=es):
    #把多個空格替換成單個空格 並把前後的空格踢掉
    target_tmNames = re.sub(' +', ' ', target_tmNames).strip()
    #自空格切開 形成一個list(在下面進階搜尋會需要 也可以到下面再split)
    target_tmNames = target_tmNames.split()
    ### 移動處 (11/15)
    resultsAAA = []
    if(mode == "same"):
        return_size = len(target_id_list)
    # ** 注意 target_tmNames 若為 [] **
    
    if(mode != "different"):
        query_body = {"query": {}}
        query_body["sort"] = [{"_score": "desc"}, {"appl-date":"desc"}]
        # 只要使用者［有］輸入任何搜尋條件（無論文字搜尋或是圖片搜尋）
        if target_tmNames != [] or target_draft_c != "" or target_draft_e != "" or target_draft_j != "" or target_classcodes != [] or target_color != "" or target_applicant != "" or target_startTime != "" or target_endTime != "" or target_id_list != [] or length != 0:
            query_body["query"]["bool"] = {}
            query_body["query"]["bool"]["should"] = []
            query_body["query"]["bool"]["filter"] = []
            query_body["query"]["bool"]["must_not"] = []
        else:  # 只要使用者［沒有］輸入任何搜尋條件，不作篩選
            query_body["query"]["match_all"] = {}

        # 【有指定搜尋】商標名稱｜支援空白分隔
        if target_tmNames != []:
            for akeyword in target_tmNames:
                query_body["query"]["bool"]["should"].append({
                    "match": {
                        "tmark-name": {
                            "query": akeyword,
                            "boost": 20 #有match到這些字的權重加20
                        }
                    }
                })

        # 【有指定搜尋】申請者/單位的名稱篩選，含中文、英文、英文
        if target_applicant != "":
            q_target_applicant = re.sub(
                ' +', '', target_applicant).strip()  # 去除所有空白
            q_target_applicant = "*" + "*".join(q_target_applicant) + "*"
            searchApplicant_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_applicant,
                    "fields": [
                        "applicant-chinese-name",
                        "applicant-english-name",
                        "applicant-japanese-name"
                    ]
                }
            }

            query_body["query"]["bool"]["should"].append(searchApplicant_queryStr)

        # 以下三段是什麼意思?為何不是filter?
        # 【有指定搜尋】圖樣內文字，含中文、英文、日文，不含符號

        if target_draft_c != "":
            q_target_draft_c = re.sub(' +', ' ', target_draft_c).strip()  # 去除所有空白
            q_target_draft_c = "*" + "*".join(q_target_draft_c) + "*"

            searchChineseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_c,
                    "fields": [
                        "tmark-name",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searchChineseDraft_queryStr)

        if target_draft_e != "":
            q_target_draft_e = re.sub(' +', ' ', target_draft_e).strip()  # 去除所有空白
            q_target_draft_e = "*" + "*".join(q_target_draft_e) + "*"

            searchEnglishDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_e,
                    "fields": [
                        "tmark-draft-e",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searchEnglishDraft_queryStr)

        if target_draft_j != "":
            q_target_draft_j = re.sub(' +', ' ', target_draft_j).strip()  # 去除所有空白
            q_target_draft_j = "*" + "*".join(q_target_draft_j) + "*"

            searcJapaneseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_j,
                    "fields": [
                        "tmark-draft-j",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searcJapaneseDraft_queryStr)

        # 【有指定搜尋】商標類別
        if target_classcodes != []:
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"goodsclass-code": target_classcodes}})

        # 【有指定搜尋】商標色彩
        if target_color != "":
            if target_color == "墨色":
                query_body["query"]["bool"]["filter"].append(
                    {"terms": {"tmark-color-desc": ["墨色", "其他"]}})
            elif target_color == "彩色":
                query_body["query"]["bool"]["filter"].append(
                    {"terms": {"tmark-color-desc": ["彩色", "紅色", "咖啡色", "藍色", "其他"]}})

        # 【有指定搜尋】商標日期範圍
        if target_startTime != "" and target_endTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"gte": target_startTime, "lte": target_endTime}}})
        elif target_startTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"gte": target_startTime}}})
        elif target_endTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"lte": target_endTime}}})

        # 指定id
        if target_id_list != []:
            query_body["query"]["bool"]["filter"].append({
                "terms": {
                    "appl-no": target_id_list
                }
            })
        
        if(mode == "different_score"):
            query_body["query"]["bool"]["filter"].append(
                    {"bool": {"must_not": {"terms": {"length": [length,0]}}}})
        queryResultsCNT = travel_es(es, resultsAAA, return_size, index="logoshot2022", body=query_body)
        if(mode != "strict"):
            if(resultsAAA):
                min_score = resultsAAA[-1][-1]
                max_score = resultsAAA[0][-1]
                if(min_score != max_score):
                    for i in resultsAAA:
                        i[-1] = ((i[-1] - min_score) / (max_score - min_score))
                else:
                    for i in resultsAAA:
                        i[-1] = 0
                resultsAAA = [(item[-3], item[-2], item[-1]) for item in resultsAAA] 
        else:
            resultsAAA = [(item[-3], item[-2], item[-1]) for item in resultsAAA] 
        
    else:
        unionAAA_word = []
        unionAAA_length = []
        resultsAAA = []
        query_body_word = {"query": {}}
        query_body_word["query"]["bool"] = {}
        query_body_word["query"]["bool"]["should"] = []
        for akeyword in target_tmNames:
            query_body_word["query"]["bool"]["should"].append({
                "match": {
                    "tmark-name": {
                        "query": akeyword,
                        "boost": 20 #有match到這些字的權重加20
                    }
                }
            })
        query_body_length = {"query": {"bool": {"filter":{"bool": {"must": {"terms": {"length": [length+2,length-2]}}}}}}}
        queryResultsCNT = travel_es(es, unionAAA_length, return_size, index="logoshot2022", body=query_body_length)
        queryResultsCNT = travel_es(es, unionAAA_word, return_size, index="logoshot2022", body=query_body_word)
        unionAAA_word = [(item[0], item[1]) for item in unionAAA_word] 
        unionAAA_length = [(item[0], item[1]) for item in unionAAA_length] 
        unionAAA_word.extend(unionAAA_length)
        resultsAAA = set(unionAAA_word)

    return resultsAAA


# different_score = esQuery(mode = 'different_score', target_applicant="財金文化事業股份有限公司", length = 5)
# different = esQuery(mode = 'different', length = 5) #目前是預計回傳所有長度不同的字 資料量很大
# same = esQuery(mode = 'same',  target_applicant="財金文化事業股份有限公司", target_id_list = ["99065347", "98036240", "97015877", "97007426"])
# strict = esQuery(mode = 'strict', target_tmNames="賓果", target_color="彩色")
# same = esQuery(mode = 'same',  target_applicant="財金文化事業股份有限公司", target_id_list = ["99065347", "98036240", "97015877", "97007426"])



"""
def esQuery(mode, length = 0, target_tmNames=[], target_id_list = [], return_size = 5000, isImageSearchFilter=False, target_draft_c="", target_draft_e="", target_draft_j="", target_classcodes=[], target_color="", target_applicant="", target_startTime="", target_endTime="", es=es):    

    ### 移動處 (11/15)
    resultsAAA = []
    if(mode == "same"):
        return_size = len(target_id_list)
    elif(mode == "different"):
        return_size = sys.maxsize
    
    # Discuss 試著將query_body用Search來寫
    if(mode != "different"):
        query_body = {"query": {}}
        # 只要使用者［有］輸入任何搜尋條件（無論文字搜尋或是圖片搜尋）
        if target_tmNames != [] or target_draft_c != "" or target_draft_e != "" or target_draft_j != "" or target_classcodes != [] or target_color != "" or target_applicant != "" or target_startTime != "" or target_endTime != "" or target_id_list != [] or length != 0:
            query_body["query"]["bool"] = {}
            query_body["query"]["bool"]["should"] = []
            query_body["query"]["bool"]["filter"] = []
            query_body["query"]["bool"]["must_not"] = []
        else:  # 只要使用者［沒有］輸入任何搜尋條件，不作篩選
            query_body["query"]["match_all"] = {}

        if target_tmNames != []:
            for akeyword in target_tmNames:
                query_body["query"]["bool"]["should"].append({
                    "match": {
                        "tmark-name": {
                            "query": akeyword,
                            "boost": 20 #有match到這些字的權重加20
                        }
                    }
                })

        # 【有指定搜尋】申請者/單位的名稱篩選，含中文、英文、英文
        if target_applicant != "":
            q_target_applicant = re.sub(
                ' +', '', target_applicant).strip()  # 去除所有空白
            q_target_applicant = "*" + "*".join(q_target_applicant) + "*"
            searchApplicant_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_applicant,
                    "fields": [
                        "applicant-chinese-name",
                        "applicant-english-name",
                        "applicant-japanese-name"
                    ]
                }
            }

            query_body["query"]["bool"]["should"].append(searchApplicant_queryStr)

        # 以下三段是什麼意思?為何不是filter?
        # 【有指定搜尋】圖樣內文字，含中文、英文、日文，不含符號

        if target_draft_c != "":
            q_target_draft_c = re.sub(' +', ' ', target_draft_c).strip()  # 去除所有空白
            q_target_draft_c = "*" + "*".join(q_target_draft_c) + "*"

            searchChineseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_c,
                    "fields": [
                        "tmark-name",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searchChineseDraft_queryStr)

        if target_draft_e != "":
            q_target_draft_e = re.sub(' +', ' ', target_draft_e).strip()  # 去除所有空白
            q_target_draft_e = "*" + "*".join(q_target_draft_e) + "*"

            searchEnglishDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_e,
                    "fields": [
                        "tmark-draft-e",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searchEnglishDraft_queryStr)

        if target_draft_j != "":
            q_target_draft_j = re.sub(' +', ' ', target_draft_j).strip()  # 去除所有空白
            q_target_draft_j = "*" + "*".join(q_target_draft_j) + "*"

            searcJapaneseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
                "query_string": {  # 支援通配符，例如 "*南*普*公司"
                    "query": q_target_draft_j,
                    "fields": [
                        "tmark-draft-j",
                    ]
                }
            }
            query_body["query"]["bool"]["should"].append(searcJapaneseDraft_queryStr)

        # 【有指定搜尋】商標類別
        if target_classcodes != []:
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"goodsclass-code": target_classcodes}})

        # 【有指定搜尋】商標色彩
        if target_color != "":
            if target_color == "墨色":
                query_body["query"]["bool"]["filter"].append(
                    {"terms": {"tmark-color-desc": ["墨色", "其他"]}})
            elif target_color == "彩色":
                query_body["query"]["bool"]["filter"].append(
                    {"terms": {"tmark-color-desc": ["彩色", "紅色", "咖啡色", "藍色", "其他"]}})

        # 【有指定搜尋】商標日期範圍
        if target_startTime != "" and target_endTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"gte": target_startTime, "lte": target_endTime}}})
        elif target_startTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"gte": target_startTime}}})
        elif target_endTime != "":
            query_body["query"]["bool"]["should"].append(
                {"range": {"appl-date": {"lte": target_endTime}}})

        # 指定id
        if target_id_list != []:
            query_body["query"]["bool"]["filter"].append({
                "terms": {
                    "appl-no": target_id_list
                }
            })
        
        if(mode == "different_score"):
            query_body["query"]["bool"]["filter"].append(
                    {"bool": {"must_not": {"terms": {"length": [length,0]}}}})
        
        queryResultsCNT = travel_es(es, resultsAAA, return_size, index="logoshot2022", body=query_body)

        if(mode != "strict"):
            if resultsAAA != []:
                min_score = resultsAAA[-1][-1]
                max_score = resultsAAA[0][-1]
                if(min_score != max_score):
                    for i in resultsAAA:
                        i[-1] = ((i[-1] - min_score) / (max_score - min_score)) * 2 - 1
                else:
                    for i in resultsAAA:
                        i[-1] = -1 #TODO: maybe 0
            resultsAAA = [(item[-3], item[-2], item[-1]) for item in resultsAAA] 
            if(mode == "same"):
                resultsAAA = sorted(resultsAAA, key=lambda x: target_id_list.index(x[0]))
        else:
            resultsAAA = [(item[-3], item[-2], item[-1]) for item in resultsAAA] 
        
    else:
        query_body = {"query": {"bool": {"filter":{"bool": {"must_not": {"terms": {"length": [length,0]}}}}}}}
        queryResultsCNT = travel_es(es, resultsAAA, return_size, index="logoshot2022", body=query_body)
        resultsAAA = [(item[0], item[1]) for item in resultsAAA] 
    print(f"esQuery done, {len(resultsAAA)} records in total")
    if len(resultsAAA) == 0:
        print("resultsAAA length == 0")
        # print("qqqqqqqq", query_body)

    return resultsAAA

def map_index(x, target_id_list):
    print("type", type(x[0]), x[0])
    if x[0] != None:
        try:
            return target_id_list.index(x[0])
        except:
            pass
    else:
        pass
    #  target_id_list.index(x[0])
"""