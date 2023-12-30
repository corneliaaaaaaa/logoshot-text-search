import time
import warnings
import pandas as pd
from pymilvus import Collection, utility
from elasticsearch import Elasticsearch
from utils.utils import keyword_preprocess, sum_scores, process_results
from utils.sms.sequence_matcher_scoring import sequence_matcher_scoring
from utils.es_search import esQuery, get_final_result
from utils.milvus import connect_to_milvus, get_collection, search

# connect with elasticsearch
es = Elasticsearch(
    hosts="trueint.lu.im.ntu.edu.tw",
    port=9200,
    timeout=180
)

# connect with milvus
connect_to_milvus()
ll = utility.list_collections()
for c in ll:
    collection = Collection(c)
    print("collection", c)
    collection.release()

# load milvus collections
print("collection release done")
pinyin_collection = get_collection('pinyin_embedding_300_L2')
print("get pinyin data collection done")
glyph_collection = get_collection('glyph_embedding_3619_no_length')
print("get glyph data collection done")
pinyin_unit_collection = get_collection('pinyin_embedding_unit')
print("get pinyin unit collection done")
glyph_unit_collection = get_collection('glyph_embedding_unit')
print("get glyph unit collection done")

df = pd.read_csv(
    "/home/ericaaaaaaa/logoshot/utils/vector/glyph/CNS_SUMMARY_TABLE.csv", encoding="utf8")


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
    sms_threshold = 0.7
    final_return_size = 400
    es_return_size = 1000
    es_return_size_strict = final_return_size
    if glyph:
        milvus_threshold = 0.7
        str_mode = "形"
        milvus_return_size = 2000
        es_return_size_3 = 1000
        es_return_size_4 = 1000
        data_collection = glyph_collection
        unit_collection = glyph_unit_collection
    else:
        milvus_threshold = -5   # the threshold to decide whether we only need to run milvus
        str_mode = "音"
        milvus_return_size = 10000
        es_return_size_3 = 1000
        es_return_size_4 = 1000
        data_collection = pinyin_collection
        unit_collection = pinyin_unit_collection
        sms_threshold = -0.3
    # if score of that index > threshold, there's no need for further searching
    milvus_key_index = 0
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
    weight = 1
    nprobe = 1000

    # preprocess
    target_tmNames = keyword_preprocess(target_tmNames)

    st = time.time()

    # strict search
    if target_tmNames != "" and glyph is False and pinyin is False:
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
            return_size=es_return_size_strict,
        )
        results = es_results
        map_tmName = False
        caseType = "嚴格搜尋"
    # 形近 / 音近搜尋
    else:
        # milvus query
        if pinyin:
            milvus_results = search(size=milvus_return_size, nprobe=nprobe,
                                    target=target_tmNames, collection=data_collection, type="L2")
        else:
            milvus_results = search(size=milvus_return_size, nprobe=nprobe,
                                    target=target_tmNames, collection=data_collection, type="IP")
        milvus_time = time.time() - st
        # sort the items with same score with id, descending
        milvus_results = sorted(
            milvus_results, key=lambda item: (-item[1], item[0]))
        print(
            f"milvus_results: {len(milvus_results)} records, spent {milvus_time:.4f} s")
        print(milvus_results[:data_shown])

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
                print(
                    f"mode: {str_mode}近搜尋、有其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
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
                es_time = time.time() - milvus_time - st
                print(
                    f"es results: {len(es_results)} records, spent {es_time:.4f} s")
                print(es_results[:data_shown])

                # sum es score and milvus score
                results = sum_scores(milvus_results, es_results, False)
                sum_time = time.time() - es_time - milvus_time - st
                print(
                    f"summed results: {len(results)} records, spent {sum_time:.4f} s")
                print(results[:data_shown])

                caseType = "milvus (> threshold) + es (other filter)"
            else:
                print(
                    f"mode: {str_mode}近搜尋、無其他搜尋條件 (milvus 有相似度 > threshold 的結果)")
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
                print(
                    f"mode: {str_mode}近搜尋、有其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
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
                es_time_same = time.time() - milvus_time - st
                print(
                    f"es_results: {len(es_results)} records, spent {es_time_same:.4f} s")
                print(es_results[:data_shown])

                # sum es score and milvus score
                same_length_results = sum_scores(
                    milvus_results, es_results, False)
                sum_time_same = time.time() - es_time_same - milvus_time - st
                print(
                    f"summed same_length_results: {len(same_length_results)} records, spent {sum_time_same:.4f} s")
                print(same_length_results[:data_shown])

                # deal with trademarks of different length from the keyword
                # elastic search by length of the target trademark name
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
                    return_size=es_return_size_3,
                    length=len(target_tmNames),
                )
                es_time_diff = time.time() - es_time_same - sum_time_same - milvus_time - st
                print(
                    f"es_results: {len(es_results)} records, spent {es_time_diff:.4f} s")
                print(es_results[:data_shown])

                # get the id and trademark
                es_results_id = [appl_no for appl_no,
                                 tmName, score in es_results]
                es_results_tmName = [tmName for appl_no,
                                     tmName, score in es_results]

                # sequence matcher scoring
                sms_results = sequence_matcher_scoring(
                    es_results_id, es_results_tmName, target_tmNames, sms_threshold, glyph, unit_collection)
                sms_time = time.time() - es_time_diff - es_time_same - \
                    sum_time_same - milvus_time - st
                print(f"sms spent {sms_time:.4f} s")

                # weight milvus results
                if not glyph:
                    sms_top_score = sms_results[0][1]
                    if sms_top_score == 1:
                        sms_top_score = 0.99999
                    weight = (milvus_results[0][1] +
                              0.0001) / (1 - sms_top_score)
                    sms_results = [(appl_no, (1 - score) * weight)
                                   for appl_no, score in sms_results]
                else:
                    sms_results = [(appl_no, score * weight)
                                   for appl_no, score in sms_results]

                # sum es score and sms score
                different_length_results = sum_scores(
                    sms_results, es_results, False)
                sum_time_diff = time.time() - sms_time - es_time_diff - es_time_same - \
                    sum_time_same - milvus_time - st
                print(
                    f"summed different_length_results: {len(different_length_results)} records,"
                    f"spent {sum_time_diff:.4f} s")
                print(different_length_results[:data_shown])

                # combine results of trademarks of same and different length
                results.extend(same_length_results)
                results.extend(different_length_results)
                caseType = "milvus (< threshold) + es (other filter) + sms"
            else:
                print(
                    f"mode: {str_mode}近搜尋、無其他搜尋條件 (milvus 無相似度 > threshold 的結果)")
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
                    return_size=es_return_size_4,
                )
                es_time = time.time() - milvus_time - st
                print(
                    f"es_results: {len(es_results)} records, spent {es_time:.4f} s")
                print(es_results[:data_shown])

                # get the id and trademark name
                es_results_id = [appl_no for appl_no, tmName in es_results]
                es_results_tmName = [tmName for appl_no, tmName in es_results]

                # sequence matcher scoring
                sms_results = sequence_matcher_scoring(
                    es_results_id, es_results_tmName, target_tmNames, sms_threshold, glyph, unit_collection)
                sms_time = time.time() - es_time - milvus_time - st
                print(f"sms spent {sms_time:.4f}")

                # weight milvus results
                if not glyph:
                    sms_top_score = sms_results[0][1]
                    if sms_top_score == 1:
                        sms_top_score = 0.99999
                    weight = (milvus_results[0][1] +
                              0.0001) / (1 - sms_top_score)
                    sms_results = [(appl_no, (1 - score) * weight)
                                   for appl_no, score in sms_results]
                else:
                    sms_results = [(appl_no, score * weight)
                                   for appl_no, score in sms_results]

                # combine results of trademarks of same and different length
                results.extend(milvus_results)
                results.extend(sms_results)
                caseType = "milvus (< threshold) + es (only query) + sms"

        # sort results by score
        results = process_results(results)
    results = results[:final_return_size]
    print(f"final results: {len(results)} records")
    print(results[:data_shown])

    # query full document of each result if not strict mode
    if glyph or pinyin:
        results_id_list = [appl_no for appl_no, tmName in results]
        if results_id_list != []:
            final_results = get_final_result(es, results_id_list)
        else:
            final_results = results
    else:
        final_results = results

    et = time.time()
    print(f"text search spent {et - st: .4f} s")

    print(f"results returned to FE: {len(final_results)} records")
    print(final_results[:data_shown])

    return final_results
