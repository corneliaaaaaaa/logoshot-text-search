# 搜尋順位:
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json
import pandas as pd
import re
import dimsim  # 音近字
import time
from datetime import datetime
import sys

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

# df = pd.read_csv("CNS_SUMMARY_TABLE.csv", encoding="utf8")
df = pd.read_csv("/home/ericaaaaaaa/logoshot/CNS_SUMMARY_TABLE.csv", encoding="utf8")
# print(df)


def get_object_size(obj):
    """Recursively calculates the memory used by an object and all nested elements."""
    size = sys.getsizeof(obj)

    if size is None:
        return 0
    elif isinstance(obj, dict):
        size += sum(
            get_object_size(key) + get_object_size(value) for key, value in obj.items()
        )
    elif isinstance(obj, (list, tuple, set)):
        size += sum(get_object_size(item) for item in obj)
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        size += sum(get_object_size(item) for item in obj)

    return size


# example usage
my_dict = {"key1": {"subkey1": [1, 2, 3], "subkey2": {"nestedkey1": "value1"}}}
size = get_object_size(my_dict)
print(f"Memory used by object: {size} bytes")


def toComponents(trademarkName, df=df):
    """算出商標名稱每個字的 components 組成的 list"""
    targetTM = trademarkName
    targetTMComponentsList = []  # 商標名稱 components
    try:
        # 商標名稱只保留中文
        targetTM = re.sub(
            r"[^\u4e00-\u9fa5]", "", targetTM
        )  # resub 將指定字串 substitute 成別的，取非^ + 漢字範圍\u4e00-\u9fa5
    except:
        pass
    if targetTM:
        # 逐字對應 CNS_SUMMARY_TABLE，從中找到每個字的 component list，並都合併到 output list
        for word in targetTM:
            try:
                component = df.loc[(df["TEXT"] == word)].iloc[0].at["COMPONENTS"]
                if component != "###":
                    componentList = component.split(
                        ","
                    )  # 編號們變 list ("11, 12" -> [11, 12])
                    targetTMComponentsList.extend(componentList)
                else:
                    pass
            except:
                pass
    return targetTMComponentsList


def intersection_list(list_1, list_2):
    """找兩個 component list 的交集"""
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


def travel_es(es, result_list, **kwargs):
    """
    依條件搜尋，並遍歷結果。es search 是有數量限制的，所以要用 scroll 實現遍歷搜尋結果的功能。

    ===
    input
    - es
    - result_list: list, 儲存 es 的搜尋結果
    - kwargs: arguments same as es api. (index & body)
    output
    - total_size: 搜尋結果資料筆數
    """
    kwargs.setdefault("scroll", "2m")  # scroll: 游標開啟的時長，足夠處理一批資料即可，太長會耗資源
    kwargs.setdefault("size", 1000)  # size: 每次回傳幾筆

    res = es.search(**kwargs)

    # 初始化: 把所有符合搜索條件的結果存起來，遍歷時就從這邊抓資料
    sid = res["_scroll_id"]
    print("sid:", sid)
    scroll_size = len(res["hits"]["hits"])
    print("scroll_size:", scroll_size)
    total_size = scroll_size
    print("total_size:", total_size)
    result_list.append(res["hits"]["hits"])

    # 遍歷: 拿 scroll_id 定位，取出 size 筆資料
    if total_size <= 10000:
        while scroll_size > 0:
            "Scrolling..."
            data = es.scroll(scroll_id=sid, scroll="4m")

            # update scroll_id
            sid = data["_scroll_id"]

            # get results returned in the last scroll
            result_list.append(data["hits"]["hits"])
            scroll_size = len(data["hits"]["hits"])
            total_size += scroll_size
            # print("total_size:", total_size)

            # 不要讓程式搜尋太多不必要的結果
            # if total_size >= 2:
            #     break

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
    """
    執行文字搜尋的完整流程，包含建立搜尋條件的 query_body、初步搜尋、音近字/形近字搜尋、給分數、
    依分數排序，最終回傳符合條件的結果。

    ===
    input
    - 關鍵詞: searchKeywords, target_draft_c,...
    - 是否選擇音近字/形近字搜尋: isSimSound, isSimShape
    - es
    output
    - final_result: list, 儲存符合條件的商標的 document
    """
    searchKeywords = re.sub(" +", " ", searchKeywords).strip()
    searchKeywords = searchKeywords.split()  # 可以利用空格一次搜尋多個關鍵詞
    resultsAAA = []
    query_body = {"query": {}}

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

    # 如果使用者［有］輸入搜尋條件，就會有不同種的篩選 & 對應的計分機制
    # if (
    #     searchKeywords != []
    #     or target_draft_c != ""
    #     or target_draft_e != ""
    #     or target_draft_j != ""
    #     or target_classcodes != []
    #     or target_color != ""
    #     or target_applicant != ""
    #     or target_startTime != ""
    #     or target_endTime != ""
    # ):
    #     query_body["query"]["bool"] = {}
    #     query_body["query"]["bool"]["must"] = []  # 必須都滿足且算分 (目前沒用到)
    #     query_body["query"]["bool"]["should"] = []  # 滿足其中一個且算分
    #     query_body["query"]["bool"]["filter"] = []  # 必須滿足但不算分
    # # 如果使用者［沒有］輸入任何搜尋條件，就查詢所有數據 (但為了得到理想的搜尋結果，使用者目前被要求一定要下搜尋條件，所以不會掉進來)
    # else:
    query_body["query"]["match_all"] = {}

    # 定義哪個搜尋條件適用哪種篩選 (should or filter)
    # 以下是使用 should 的條件
    # 商標名稱
    if searchKeywords != []:
        for akeyword in searchKeywords:
            query_body["query"]["bool"]["should"].append(
                {
                    "match": {  # 商標名稱一定包含關鍵詞的至少一個字
                        "tmark-name": {"query": akeyword, "boost": 20}  # TODO 分數怎麼訂更好
                    }
                }
            )
    # 申請者或單位名稱，含中文、英文、日文
    if target_applicant != "":
        q_target_applicant = re.sub(" +", "", target_applicant).strip()  # 去除所有空白
        q_target_applicant = "*" + "*".join(q_target_applicant) + "*"
        print(q_target_applicant, "###################")
        searchApplicant_queryStr = {
            "query_string": {  # 申請者名稱一定是 *南*普*公*司 (* 可以代表多個字元)，而選擇用 query_string 是因它支援通配符 (*)
                "query": q_target_applicant,
                "fields": [
                    "applicant-chinese-name",
                    "applicant-english-name",
                    "applicant-japanese-name",
                ],
            }
        }
        query_body["query"]["bool"]["should"].append(searchApplicant_queryStr)
    # 圖樣內文字，含中文、英文、日文，不含符號
    if target_draft_c != "":
        q_target_draft_c = re.sub(" +", " ", target_draft_c).strip()  # 去除所有空白
        q_target_draft_c = "*" + "*".join(q_target_draft_c) + "*"
        print("【q_target_draft_c】", q_target_draft_c)
        searchChineseDraft_queryStr = {
            "query_string": {
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
        searchEnglishDraft_queryStr = {
            "query_string": {
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
        searcJapaneseDraft_queryStr = {
            "query_string": {
                "query": q_target_draft_j,
                "fields": [
                    "tmark-draft-j",
                ],
            }
        }
        query_body["query"]["bool"]["should"].append(
            searcJapaneseDraft_queryStr
        )  # TODO 為甚麼這些都是用 should

    # 以下是使用 filter 的條件
    # 商標類別
    if target_classcodes != []:
        query_body["query"]["bool"]["filter"].append(
            {
                "terms": {  # 商標類別一定等於 target_classcodes
                    "goodsclass-code": target_classcodes
                }
            }
        )
    # 商標色彩
    if target_color != "":
        if target_color == "墨色":
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"tmark-color-desc": ["墨色", "其他"]}}
            )
        elif target_color == "彩色":
            query_body["query"]["bool"]["filter"].append(
                {"terms": {"tmark-color-desc": ["彩色", "紅色", "咖啡色", "藍色", "其他"]}}
            )
    # 商標日期範圍
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

    # 搜尋、紀錄結果與資料筆數
    # test memory size
    st = time.time()
    queryResultsCNT = travel_es(es, resultsAAA, index="logoshot2022", body=query_body)
    et = time.time()
    print("time for loading all data: ", et - st)
    size = get_object_size(resultsAAA)
    print("memory used by all data", size)

    # 針對初步搜尋得出的結果，每一筆都給它一個初始分數 (有 10 筆資料的話，第一名得 10 分)
    score_Result = {}  # 格式為 (tmark-name, _id, CNS_COMPONENTS, appl-no): score
    esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果
    for outerPage in resultsAAA:
        for data in outerPage:
            score_Result[
                (
                    data["_source"]["tmark-name"],
                    data["_id"],
                    tuple(data["_source"]["CNS_COMPONENTS"]),
                    data["_source"]["appl-no"],
                    # data["_score"],
                )
            ] = esQueryCNT
            esQueryCNT -= 1
    # print(*score_Result.items(), sep="\n")

    #############################################################################

    # 音近字 & 形近字篩選
    esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果
    regeTMname_target = re.sub(
        r"[^\u4e00-\u9fa5]", "", ("").join(searchKeywords)
    )  # 去調空格、只留下中文
    print("【regeTMname_target】:", regeTMname_target)
    print("The current date and time is", datetime.now())

    # 音近字
    if isSimSound == True:
        startTime = time.time()
        closeSound_result1 = []  # 關鍵詞和商標名稱長度相同(可以排序)的音近字結果
        closeSound_result2 = []  # 關鍵詞和商標名稱長度不同(無法排序)的音近字結果

        # 每一筆初步篩選得出的結果下去計算音近分數
        for outerPage in resultsAAA:
            for data in outerPage:
                try:
                    regeTMname_search = re.sub(
                        r"[^\u4e00-\u9fa5]", "", data["_source"]["tmark-name"]
                    )  # 只留下中文
                    # 兩詞長度相同，可以計算音近分數
                    if len(regeTMname_search) == len(regeTMname_target):
                        dimsimScore = dimsim.get_distance(
                            regeTMname_target, regeTMname_search
                        )  # TODO: 好像應該叫dimsimDistance
                        closeSound_result1.append(
                            (
                                data["_source"]["tmark-name"],
                                data["_id"],
                                tuple(data["_source"]["CNS_COMPONENTS"]),
                                data["_source"]["appl-no"],
                                dimsimScore,
                            )
                        )
                    # 兩詞長度不同，無法計算音近分數
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
        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果        TODO 感覺不需要，上面寫過了

        # 可以計算音近距離的資料，依照音近距離排序，距離小的在前
        closeSound_result1 = [
            data for data in sorted(closeSound_result1, key=lambda x: x[4])
        ]
        # 可以排序的音近字結果，加上不同的分數 (距離越近者，加越多分)
        for data in closeSound_result1:
            score_Result[(data[0], data[1], tuple(data[2]), data[3])] += (
                esQueryCNT * 1000
            )
            esQueryCNT -= 1
        # 不可排序的音近字結果，全部加上一樣的分數                TODO: 海底撈撈的得分 = 一二三四的得分
        for data in closeSound_result2:
            score_Result[(data[0], data[1], tuple(data[2]), data[3])] += esQueryCNT
        endTime = time.time()
        print("【音近字所耗時間(秒)】", endTime - startTime)

    # 形近字
    if isSimShape == True:
        startTime = time.time()
        closeShape_result = []
        for key in score_Result:  # TODO: why not "for outerPage in resultAAA"
            # 計算關鍵詞的 component list             TODO: 這一行好像不用放在 for 裡面
            targetTMComponentsList = toComponents(regeTMname_target)
            testTMComponentsList = list(key[2])
            # 找關鍵詞與商標名稱的 component list 之交集
            intersectComponents = intersection_list(
                targetTMComponentsList, testTMComponentsList
            )
            # 計算 component list 重疊的比例
            ratioTargetTM = 0
            ratioTestTM = 0
            if len(targetTMComponentsList) != 0:
                ratioTargetTM = len(intersectComponents) / len(targetTMComponentsList)
            if len(testTMComponentsList) != 0:
                ratioTestTM = len(intersectComponents) / len(testTMComponentsList)
            # 關鍵詞與商標名稱的比例相乘，存入結果
            closeShape_result.append(
                (key[0], key[1], key[2], key[3], ratioTargetTM * ratioTestTM)
            )
        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果             TODO: 好像不需要

        # 排序結果，比例乘積較大者在前，代表字形較相近
        closeShape_result = [
            data for data in sorted(closeShape_result, key=lambda x: -x[4])
        ]
        # 依照形近字結果，加上不同的分數 (字形越相近者，加越多分)
        for data in closeShape_result:
            score_Result[(data[0], data[1], data[2], data[3])] += esQueryCNT * 1000
            esQueryCNT -= 1

        endTime = time.time()
        print("【形近字所耗時間】", endTime - startTime)

    # 經過初步搜尋、音近字/形近字搜尋後，以資料獲得的分數進行最後的排序
    sorted_result = sorted(score_Result.items(), key=lambda x: x[1], reverse=True)
    # 於文字搜尋會依序列出，於圖片搜尋則會將結果之 _ig, img_path, appl-no 傳給圖片搜尋模型作後續處理。

    # 印出結果
    # print("sorted_result", sorted_result)
    # print(*[(data[0][0], data[0][1], data[0][3])
    #       for data in sorted_result[:100]], sep="\n")
    print(*sorted_result[:10], sep="\n")

    # 從 sorted_result 中取出每筆資料的 id，並以 id 在 elasticsearch 中搜尋對應的 doc 的完整內容
    finalResult_IDList = [data[0][1] for data in sorted_result]
    finalResult = []
    if finalResult_IDList != []:
        finalResult = es.mget(index="logoshot2022", body={"ids": finalResult_IDList})[
            "docs"
        ]
        # for d in finalResult[:100]:
        #     print(d["_source"]["tmark-name"])

    finalResultDict = {}
    print("【es ### finalResultDict】")
    print("The current date and time is", datetime.now())

    # print(finalResultDict)
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


esQuery(searchKeywords="", isSimShape=False, isSimSound=False)
# esQuery(target_classcodes=["1"])
# esQuery(searchKeywords="海低勞", isSimSound=True)
# esQuery(searchKeywords="文 化事  業股份有限公司")
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
