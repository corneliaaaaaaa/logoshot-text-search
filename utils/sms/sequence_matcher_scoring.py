import time
import pandas as pd
from datetime import datetime
import re
from utils.sms.difflib_for_comparing_similar_strings import SequenceMatcher
from utils.vector.vector import get_glyph_vector, get_pinyin_vector
import os


def get_tmName_char_vector(tmName_list: list, glyph: bool = False, unit_collection=None):
    """
    Get vectors for each tmName.
    """
    tmName_char_vec_list = []
    query_set = set()

    for tmName in tmName_list:
        tmName = re.sub(r"[^\u4e00-\u9fa5]", "", tmName)
        query_set.update([*tmName])
        query_string = str(list(query_set)).replace("'", '"')

    res = unit_collection.query(
        expr=f'word in {query_string}',
        offset=0,
        limit=len(query_set),
        output_fields=["word", "vector"],
    )

    # process response
    word_vec_dict = {}
    for item in res:
        word_vec_dict[item["word"]] = item["vector"]

    # build trademark vector list
    for tmName in tmName_list:
        tmName_char_vec = []
        tmName = re.sub(r"[^\u4e00-\u9fa5]", "", tmName)
        for char in tmName:
            char_vec = word_vec_dict.get(char, [])
            if char_vec == []:
                if glyph:
                    char_vec = get_glyph_vector(char)
                else:
                    char_vec = get_pinyin_vector(char)
            tmName_char_vec.append(char_vec)
        tmName_char_vec_list.append(tmName_char_vec)

    return tmName_char_vec_list


def sequence_matcher_scoring(
    id_list: list,
    tmName_list: list,
    target_tmName: str,
    threshold: float = 0.5,
    glyph: bool = False,
    unit_collection=None,
):
    """
    Compute the similarity between a target trademark and other trademarks.
    """
    if glyph:
        print(
            f"glyph similarity, keyword = {target_tmName}, threshold = {threshold}"
        )
    else:
        print(
            f"pinyin similarity, keyword = {target_tmName}, threshold = {threshold}"
        )

    # get vectors for each trademark name
    st = time.time()
    tmName_char_vec_list = get_tmName_char_vector(
        tmName_list, glyph, unit_collection)
    et = time.time()
    print("get vector spent", et - st)
    target_tmName_char_vec_list = get_tmName_char_vector(
        [target_tmName], glyph, unit_collection)[0]

    result_df = pd.DataFrame(
        {
            "appl_no": id_list,
            "tmName": tmName_list,
            "similarity": [0.0] * len(tmName_list),
            # "matching_blocks": [[]] * len(tmName_list),
            "tmName_char_vec": tmName_char_vec_list,
        }
    )

    # compute similarity
    for index, row in result_df.iterrows():
        sm = SequenceMatcher(
            None, target_tmName_char_vec_list, row["tmName_char_vec"], True, threshold, glyph
        )
        # result_df.at[index, "matching_blocks"] = sm.get_matching_blocks()
        result_df.at[index, "similarity"] = sm.ratio()
    result_df = result_df.sort_values(by=["similarity"], ascending=False)

    result = list(zip(result_df["appl_no"], result_df["similarity"]))
    print(result_df[["appl_no", "tmName", "similarity"]])

    return result
