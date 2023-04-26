# 搜尋順位:
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json
import pandas as pd
import re
import dimsim  # 音近字
import time
from datetime import datetime

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

df = pd.read_csv("CNS_SUMMARY_TABLE.csv", encoding="utf8")
# print(df)


def toComponents(trademarkName, df=df):
    targetTM = trademarkName
    targetTMComponentsList = []  # 商標名稱 components
    try:
        targetTM = re.sub(r"[^\u4e00-\u9fa5]", "", targetTM)  # 只保留中文
    except:
        pass
    if targetTM:
        for word in targetTM:
            try:
                component = df.loc[(df["TEXT"] == word)].iloc[0].at["COMPONENTS"]
                if component != "###":
                    componentList = component.split(",")
                    targetTMComponentsList.extend(componentList)
                else:
                    pass
            except:
                pass
    return targetTMComponentsList


def intersection_list(list_1, list_2):
    list1 = list_1.copy()
    list2 = list_2.copy()
    list3 = []
    if len(list1) < len(list2):
        for value in list1:
            if value in list2:
                list3.append(value)
                list2.remove(value)
    else:
        for value in list2:
            if value in list1:
                list3.append(value)
                list1.remove(value)
    return list3


# ES 的搜索是有數量限制的，因此利用官方提供的滾動 API 實現了一個對全量數據處理的功能


def travel_es(es, result_list, **kwargs):
    """
    遍歷es的搜索結果，並使用process_func處理返回的item
    process_func: function to process item.
    kwargs: arguments same as elasticsearch search api.
    """
    kwargs.setdefault("scroll", "2m")
    kwargs.setdefault("size", 1000)

    res = es.search(**kwargs)

    sid = res["_scroll_id"]
    print("sid:", sid)

    scroll_size = len(res["hits"]["hits"])
    print("scroll_size:", scroll_size)

    total_size = scroll_size
    print("total_size:", total_size)

    result_list.append(res["hits"]["hits"])

    # 不要讓程式搜尋太多不必要的結果
    if total_size <= 10000:
        while scroll_size > 0:
            "Scrolling..."

            # Before scroll, process current batch of hits
            #         process_func(res['hits']['hits'])

            data = es.scroll(scroll_id=sid, scroll="4m")

            # Update the scroll ID
            sid = data["_scroll_id"]
            #         print("sid:", sid)

            # Get the number of results that returned in the last scroll
            result_list.append(data["hits"]["hits"])
            scroll_size = len(data["hits"]["hits"])
            #         print("scroll_size:", scroll_size)

            total_size += scroll_size
            print("total_size:", total_size)

            # 不要讓程式搜尋太多不必要的結果
            if total_size >= 10000:
                break

    print("總查詢資料筆數:", total_size)
    return total_size


def esQuery(
    searchKeywords="",
    isImageSearchFilter=False,
    isSimSound=False,
    isSimShape=False,
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
    searchKeywords = re.sub(" +", " ", searchKeywords).strip()
    searchKeywords = searchKeywords.split()

    ### 移動處 (11/15)
    resultsAAA = []

    # es = Elasticsearch(
    #     hosts='trueint.lu.im.ntu.edu.tw', port=9200,
    #     timeout=180
    #     # 'http://localhost:9200/'  # 連線叢集，以列表的形式存放各節點的IP地址
    #     # sniff_on_start=True,    # 連線前測試
    #     # sniff_on_connection_fail=True,  # 節點無響應時重新整理節點
    #     # sniff_timeout=60,    # 設定超時時間
    #     # ignore=400,  # 忽略返回的400狀態碼
    #     # ignore=[400, 405, 502],  # 以列表的形式忽略多個狀態碼
    #     # http_auth=('elastic', 'changeme')  # 認證資訊
    # )

    # ** 注意 searchKeywords 若為 [] **

    query_body = {"query": {}}

    # 只要使用者［有］輸入任何搜尋條件（無論文字搜尋或是圖片搜尋）
    if (
        searchKeywords != []
        or target_draft_c != ""
        or target_draft_e != ""
        or target_draft_j != ""
        or target_classcodes != []
        or target_color != ""
        or target_applicant != ""
        or target_startTime != ""
        or target_endTime != ""
    ):
        query_body["query"]["bool"] = {}
        query_body["query"]["bool"]["must"] = []
        query_body["query"]["bool"]["should"] = []
        query_body["query"]["bool"]["filter"] = []
    else:  # 只要使用者［沒有］輸入任何搜尋條件，不作篩選
        query_body["query"]["match_all"] = {}

    # 【有指定搜尋】商標名稱｜支援空白分隔
    if searchKeywords != []:
        for akeyword in searchKeywords:
            query_body["query"]["bool"]["should"].append(
                {"match": {"tmark-name": {"query": akeyword, "boost": 20}}}
            )

    # 【有指定搜尋】申請者/單位的名稱篩選，含中文、英文、英文
    if target_applicant != "":
        q_target_applicant = re.sub(" +", "", target_applicant).strip()  # 去除所有空白
        q_target_applicant = "*" + "*".join(q_target_applicant) + "*"
        print(q_target_applicant, "###################")
        searchApplicant_queryStr = {  # 不要 multi_match 了，改用 query_string
            "query_string": {  # 支援通配符，例如 "*南*普*公司"
                "query": q_target_applicant,
                "fields": [
                    "applicant-chinese-name",
                    "applicant-english-name",
                    "applicant-japanese-name",
                ],
            }
        }

        query_body["query"]["bool"]["should"].append(searchApplicant_queryStr)

    # 【有指定搜尋】圖樣內文字，含中文、英文、日文，不含符號
    if target_draft_c != "":
        q_target_draft_c = re.sub(" +", " ", target_draft_c).strip()  # 去除所有空白
        q_target_draft_c = "*" + "*".join(q_target_draft_c) + "*"
        print("【q_target_draft_c】", q_target_draft_c)

        searchChineseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
            "query_string": {  # 支援通配符，例如 "*南*普*公司"
                "query": q_target_draft_c,
                "fields": [
                    "tmark-name",
                ],
            }
        }
        query_body["query"]["bool"]["should"].append(searchChineseDraft_queryStr)
    if target_draft_e != "":
        q_target_draft_e = re.sub(" +", " ", target_draft_e).strip()  # 去除所有空白
        q_target_draft_e = "*" + "*".join(q_target_draft_e) + "*"
        print("【q_target_draft_e】", q_target_draft_e)

        searchEnglishDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
            "query_string": {  # 支援通配符，例如 "*南*普*公司"
                "query": q_target_draft_e,
                "fields": [
                    "tmark-draft-e",
                ],
            }
        }
        query_body["query"]["bool"]["should"].append(searchEnglishDraft_queryStr)
    if target_draft_j != "":
        q_target_draft_j = re.sub(" +", " ", target_draft_j).strip()  # 去除所有空白
        q_target_draft_j = "*" + "*".join(q_target_draft_j) + "*"
        print("【q_target_draft_j】", q_target_draft_j)

        searcJapaneseDraft_queryStr = {  # 不要 multi_match 了，改用 query_string
            "query_string": {  # 支援通配符，例如 "*南*普*公司"
                "query": q_target_draft_j,
                "fields": [
                    "tmark-draft-j",
                ],
            }
        }
        query_body["query"]["bool"]["should"].append(searcJapaneseDraft_queryStr)

    # 【有指定搜尋】商標類別
    if target_classcodes != []:
        query_body["query"]["bool"]["filter"].append(
            {"terms": {"goodsclass-code": target_classcodes}}
        )

    # 【有指定搜尋】商標色彩
    if target_color != "":
        if target_color == "墨色":
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"tmark-color-desc": ["墨色", "其他"]}}
            )
        elif target_color == "彩色":
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"tmark-color-desc": ["彩色", "紅色", "咖啡色", "藍色", "其他"]}}
            )

    # 【有指定搜尋】商標日期範圍
    if target_startTime != "" and target_endTime != "":
        query_body["query"]["bool"]["filter"].append(
            {"range": {"appl-date": {"gte": target_startTime, "lte": target_endTime}}}
        )
    elif target_startTime != "":
        query_body["query"]["bool"]["filter"].append(
            {"range": {"appl-date": {"gte": target_startTime}}}
        )
    elif target_endTime != "":
        query_body["query"]["bool"]["filter"].append(
            {"range": {"appl-date": {"lte": target_endTime}}}
        )

    print("【query_body】\n", query_body)

    ########### 對每一筆初步搜尋出來的結果建一個算分數的 Dict => score_Result ###########
    # 查詢 & 紀錄查詢結果之資料筆數
    queryResultsCNT = travel_es(es, resultsAAA, index="logoshot2022", body=query_body)

    # 移動 es 位置後(11/16)

    # 之後每筆資料為 (tmark-name, _id, CNS_COMPONENTS, appl-no): score。為了計算搜尋結果的排列順序
    score_Result = {}
    esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果

    for outerPage in resultsAAA:
        for data in outerPage:
            score_Result[
                (
                    data["_source"]["tmark-name"],
                    data["_id"],
                    tuple(data["_source"]["CNS_COMPONENTS"]),
                    data["_source"]["appl-no"],
                )
            ] = esQueryCNT
            esQueryCNT -= 1

    # print(*score_Result.items(), sep="\n")

    #############################################################################
    # 【有指定搜尋】音近字
    esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果
    regeTMname_target = re.sub(r"[^\u4e00-\u9fa5]", "", ("").join(searchKeywords))
    print("【regeTMname_target】:", regeTMname_target)
    print("The current date and time is", datetime.now())

    if isSimSound == True:
        startTime = time.time()
        closeSound_result1 = []  # 可以排序的音近字結果(接前面)
        closeSound_result2 = []  # 無法排序的音近字結果(接後面)

        # 因為要確保字數和 targetSound 相同，以及確保商標名稱只看中文，所以要先篩選
        for outerPage in resultsAAA:
            for data in outerPage:

                # print(data["_source"]["tmark-name"])
                try:
                    regeTMname_search = re.sub(
                        r"[^\u4e00-\u9fa5]", "", data["_source"]["tmark-name"]
                    )
                    if len(regeTMname_search) == len(regeTMname_target):
                        # 存入 data 和 音近分數
                        dimsimScore = dimsim.get_distance(
                            regeTMname_target, regeTMname_search
                        )
                        closeSound_result1.append(
                            (
                                data["_source"]["tmark-name"],
                                data["_id"],
                                tuple(data["_source"]["CNS_COMPONENTS"]),
                                data["_source"]["appl-no"],
                                dimsimScore,
                            )
                        )
                    else:
                        closeSound_result2.append(
                            (
                                data["_source"]["tmark-name"],
                                data["_id"],
                                tuple(data["_source"]["CNS_COMPONENTS"]),
                                data["_source"]["appl-no"],
                            )
                        )
                except:
                    pass
        # 因為要確保字數和 targetSound 相同，以及確保商標名稱只看中文，所以要先篩選

        closeSound_result1 = [
            data for data in sorted(closeSound_result1, key=lambda x: x[4])
        ]  # 依序儲存音最近的搜尋結果，分數低的在前

        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果

        # 可以排序的音近字結果，依序給分
        for data in closeSound_result1:
            # print("音###:", (data[0], data[1]))
            score_Result[(data[0], data[1], tuple(data[2]), data[3])] += (
                esQueryCNT * 1000
            )
            esQueryCNT -= 1
        # 不可排序的音近字結果，全部同分
        for data in closeSound_result2:
            score_Result[(data[0], data[1], tuple(data[2]), data[3])] += esQueryCNT
        endTime = time.time()
        print("【音近字所耗時間(秒)】", endTime - startTime)
    #############################################################################
    # 【有指定搜尋】形近字

    if isSimShape == True:
        startTime = time.time()
        closeShape_result = []
        for key in score_Result:
            targetTMComponentsList = toComponents(regeTMname_target)
            testTMComponentsList = list(key[2])
            # print(testTMComponentsList)
            intersectComponents = intersection_list(
                targetTMComponentsList, testTMComponentsList
            )

            ratioTargetTM = 0
            ratioTestTM = 0
            if len(targetTMComponentsList) != 0:
                ratioTargetTM = len(intersectComponents) / len(targetTMComponentsList)
            if len(testTMComponentsList) != 0:
                ratioTestTM = len(intersectComponents) / len(testTMComponentsList)
            # print((key, ratioTargetTM * ratioTestTM))
            closeShape_result.append(
                (key[0], key[1], key[2], key[3], ratioTargetTM * ratioTestTM)
            )

        closeShape_result = [
            data for data in sorted(closeShape_result, key=lambda x: -x[4])
        ]  # 依序儲存字型最近的搜尋結果，分數大的在前
        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果

        # 可以排序的形近字結果，依序給分
        for data in closeShape_result:
            score_Result[(data[0], data[1], data[2], data[3])] += esQueryCNT * 1000
            esQueryCNT -= 1

        endTime = time.time()
        print("【形近字所耗時間】", endTime - startTime)

    # 【最後結果的排序】
    # 於文字搜尋會依序列出。
    # 於圖片搜尋則會將結果之 _ig, img_path, appl-no 傳給圖片搜尋模型作後續處理。
    sorted_result = sorted(score_Result.items(), key=lambda x: x[1], reverse=True)

    print("【es ### sorted_result】")
    print("The current date and time is", datetime.now())
    # 【印出全部】
    # print(sorted_result)

    # 【印出前百名】
    # print(*[(data[0][0], data[0][1], data[0][3])
    #       for data in sorted_result[:100]], sep="\n")
    print(*sorted_result[:10], sep="\n")

    finalResult_IDList = [data[0][1] for data in sorted_result]
    # print(finalResult_IDList)

    finalResult = []
    if finalResult_IDList != []:
        # 取回最後結果 id 在 elastic search 中的所有內容
        finalResult = es.mget(index="logoshot2022", body={"ids": finalResult_IDList})[
            "docs"
        ]
        # for d in finalResult[:100]:
        #     print(d["_source"]["tmark-name"])

    finalResultDict = {}
    print("【es ### finalResultDict】")
    print("The current date and time is", datetime.now())

    # print(finalResultDict)
    print("@@@@@@@@@@@@@@@@@@@@@@", searchKeywords)
    # es.transport.close()

    # if isImageSearchFilter == True:
    #     # print(finalResultDict)
    #     for aData in finalResult:
    #         finalResultDict[aData['_id']] = [aData['_source']['tmark-image-url_1'], aData['_source']['tmark-image-url_2'], aData['_source']
    #                                         ['tmark-image-url_3'], aData['_source']['tmark-image-url_4'], aData['_source']['tmark-image-url_5'], aData['_source']['tmark-image-url_6']]

    #     return finalResultDict
    # else:
    # print(finalResult)
    # print(JSON.jsonify(finalResult))

    # print(finalResult)
    return finalResult


# esQuery(target_classcodes=["1"])
# esQuery(target_applicant="財金文化事業股份有限公司")
# esQuery(target_startTime="2010/01/01")
# esQuery(target_endTime="2010/01/01")
# esQuery(target_color="彩色")
# esQuery(target_color="墨色")
# esQuery(searchKeywords="鼎泰豐", target_classcodes=["43"], isSimShape=True, isSimSound=True) #95025
# esQuery(target_classcodes=["43"]) #95025
# esQuery(searchKeywords="及圖")
# esQuery(searchKeywords="及圖", target_draft_j="すき", isImageSearchFilter=False)
# esQuery(searchKeywords="海低勞", isSimShape=True)
# esQuery(searchKeywords="頂泰瘋", isSimSound=True)
# esQuery(searchKeywords="a")
# esQuery(searchKeywords="賓果")
# esQuery(searchKeywords="可 可樂")
# esQuery(target_applicant="南克普布里茲有限公司")
# esQuery(target_applicant="南克里茲")
# esQuery(target_applicant="南克 茲")
# esQuery(searchKeywords="賓果", isSimShape=True)
# esQuery(target_applicant="資訊公司", target_draft_c="民", target_draft_e="ROAD")
# esQuery(searchKeywords="這", target_color="彩色")
# print(json.dumps(resultsAAA[0][0], indent=2, ensure_ascii=False))
