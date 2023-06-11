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
    milvus_threshold = 0.8   # the threshold to decide whether we only need to run milvus
    sms_threshold = 0.5
    es_return_size = 5000
    sms_return_size = 1000
    data_shown = 10
    target_id_list = []
    results = []
    milvus_results = []
    es_results = []
    sms_results = []
    same_length_results = []
    different_length_results = []
    final_results = []

    # preprocess
    target_tmName_list = keyword_preprocess(target_tmNames)
    target_tmName = target_tmName_list[0]

    #TODO: print 搜尋條件?
    
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
    # 形近 / 音近搜尋
    else:
        # milvus query
        # connect_to_milvus()
        # get_collection('pinyin_embedding_300')
        # milvus_results = search(nprobe=100, target=target_tmName, collection=collection)
        # print("milvus_results", milvus_results)
        milvus_results = [
            # ('73017927', 0.8379),
            # ('65000920', 0.8379),
            ('73017946', 0.7379),
            ('71006486', 0.7379),
            ('95031746', 0.11862),
            ('95031742', 0.11862),
            ('108084591', 0.11862),
            ('84046999', 0.11862),
            ('109059777', 0.11862),
            ('95031741', 0.11862)
        ]

        # 判斷是否需要進行更多搜尋 
        if milvus_results[0][1] > milvus_threshold:
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
                print("target_id_list", target_id_list)
                
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
            else:
                print("mode: 音近搜尋、無其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
                results = milvus_results         
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
                print("mode: 音近搜尋、有其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
                # 針對長度相同的商標名稱
                # 取得這些商標名稱在其他條件中的 es search 得分
                print("長度相同")
                target_id_list = list(map(lambda x: x[0], milvus_results))
                print("target_id_list", target_id_list)
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
                partial = 100000
                es_results_id = [appl_no for appl_no, tmName in es_results[:partial]]
                es_results_tmName = [tmName for appl_no, tmName in es_results[:partial]]
                
                # 對所有資料進行 sequence_matcher_scoring()
                st = time.time()
                sms_results = sequence_matcher_scoring(es_results_id, es_results_tmName, target_tmName, sms_threshold, glyph)
                print("sms_results", sms_results[:data_shown])
                et = time.time()
                print("sms time used", et - st)

                # 合併長度相同與長度不同的結果，並排序
                results.extend(milvus_results)
                results.extend(sms_results[:sms_return_size])
                results = sorted(results, key= lambda x: x[1], reverse=True)

    print("combined results:", len(results), "records")
    
    # query full document of each result
    results_id_list = [appl_no for appl_no, tmName in results]
    # check results (TODO: will be removed)
    check_results = esQuery(
        es=es,
        mode="same",
        target_id_list=results_id_list,
        return_size=es_return_size,
    )
    check_results = [
        (appl_no, tmName, score)
        for (appl_no, score), (appl_no, tmName) in zip(results, check_results)
    ]
    print("results")
    print(check_results[:data_shown])
    # get full documents
    if results_id_list != []:
        final_results = es.mget(index="logoshot2022", body={"ids": results_id_list})["docs"]

    return final_results
    """
    finalResultDict = {}
    print("【es ### finalResultDict】")
    print("The current date and time is", datetime.now())

    ############################################################
    # old-version
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
    for data in resultsAAA:
        score_Result[data] = esQueryCNT
        esQueryCNT -= 1
    print("score result", len(score_Result))
    # print(*score_Result.items(), sep='\n')

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
        for data in resultsAAA:
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
                            data[0],
                            data[1],
                            data[2],
                            dimsimScore,
                        )
                    )
                # 兩詞長度不同，無法計算音近分數
                else:
                    closeSound_result2.append(data)
            except:
                pass
        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果        TODO 感覺不需要，上面寫過了

        # 可以計算音近距離的資料，依照音近距離排序，距離小的在前
        closeSound_result1 = [
            (data[0], data[1], data[2])
            for data in sorted(closeSound_result1, key=lambda x: x[3])
        ]
        # 可以排序的音近字結果，加上不同的分數 (距離越近者，加越多分)
        for data in closeSound_result1:
            score_Result[data] += esQueryCNT * 1000
            esQueryCNT -= 1
        # 不可排序的音近字結果，全部加上一樣的分數                TODO: 海底撈撈的得分 = 一二三四的得分
        for data in closeSound_result2:
            score_Result[data] += esQueryCNT
        endTime = time.time()
        print("【音近字所耗時間(秒)】", endTime - startTime)

    # 形近字
    if isSimShape == True:
        startTime = time.time()
        closeShape_result = []
        # 計算關鍵詞的 component list
        targetTMComponentsList = toComponents(regeTMname_target)
        for key in score_Result:
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
                (key[0], key[1], key[2], ratioTargetTM * ratioTestTM)
            )
        esQueryCNT = queryResultsCNT  # 複製一份，以免動到真實結果             TODO: 好像不需要

        # 排序結果，比例乘積較大者在前，代表字形較相近
        closeShape_result = [
            (data[0], data[1], data[2])
            for data in sorted(closeShape_result, key=lambda x: -x[3])
        ]
        # 依照形近字結果，加上不同的分數 (字形越相近者，加越多分)
        for data in closeShape_result:
            score_Result[data] += esQueryCNT * 1000
            esQueryCNT -= 1

        endTime = time.time()
        print("【形近字所耗時間】", endTime - startTime)

    return finalResult
"""
# tests
# text_search(glyph=False, pinyin=False, target_tmNames="海底勞")

# milvus score > threshold
# text_search(glyph=False, pinyin=True, target_tmNames="海底勞", target_color="墨色") # 有其他條件
# text_search(glyph=False, pinyin=True, target_tmNames="海底勞") # 無其他條件

# no milvus score > threshold
# text_search(glyph=False, pinyin=True, target_tmNames="海底勞",  target_applicant="公司") # 有其他條件
text_search(glyph=False, pinyin=True, target_tmNames="海底勞") # 無其他條件



# esQuery(searchKeywords="", isSimShape=False, isSimSound=False)
# esQuery(target_classcodes=['1'])
# esQuery(searchKeywords="海低勞", isSimSound=True)
# esQuery(searchKeywords='文 化事  業股份有限公司')
# esQuery(target_startTime='2010/01/01')
# esQuery(target_endTime='2010/01/01')
# esQuery(target_color='彩色')
# esQuery(target_color='墨色')
# esQuery(searchKeywords='鼎泰豐', target_classcodes=['43'], isSimShape=True, isSimSound=True) #95025
# esQuery(target_classcodes=['43']) #95025
# esQuery(searchKeywords='及圖')
# esQuery(searchKeywords='及圖', target_draft_j='すき', isImageSearchFilter=False)
# esQuery(searchKeywords='海低勞', isSimShape=True)
# esQuery(searchKeywords='頂泰瘋', isSimSound=True)
# esQuery(searchKeywords='a')
# esQuery(searchKeywords='賓果')
# esQuery(searchKeywords='可 可樂')
# esQuery(target_applicant='南克普布里茲有限公司')
# esQuery(target_applicant='南克里茲')
# esQuery(target_applicant='南克 茲')
# esQuery(searchKeywords='賓果', isSimShape=True)
# esQuery(target_applicant='資訊公司', target_draft_c='民', target_draft_e='ROAD')
# esQuery(searchKeywords='這', target_color='彩色')
# print(json.dumps(resultsAAA[0][0], indent=2, ensure_ascii=False))
