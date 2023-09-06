import pandas as pd
from datetime import datetime
import re
from utils.sms.difflib_for_comparing_similar_strings import SequenceMatcher
from utils.vector.vector import get_glyph_vector, get_pinyin_vector

def get_tmName_char_vector(tmName_list: list, glyph: bool = False):
    """
    Get vectors for each tmName. TODO: may be changed to db query
    """
    tmName_char_vec_list = []
    for tmName in tmName_list:
        tmName_char_vec = []
        tmName = re.sub(r"[^\u4e00-\u9fa5]", "", tmName)
        for char in tmName:
            if glyph:
                tmName_char_vec.append(get_glyph_vector(char))
            else:
                tmName_char_vec.append(get_pinyin_vector(char))
        tmName_char_vec_list.append(tmName_char_vec)

    return tmName_char_vec_list


def sequence_matcher_scoring(
    id_list: list,
    tmName_list: list,
    target_tmName: str,
    threshold: float=0.5,
    glyph: bool=False,
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
    tmName_char_vec_list = get_tmName_char_vector(tmName_list, glyph)
    target_tmName_char_vec_list = get_tmName_char_vector([target_tmName], glyph)[0]

    result_df = pd.DataFrame(
        {
            "appl_no": id_list,
            "tmName": tmName_list,
            "similarity": [0.0] * len(tmName_list),
            "matching_blocks": [[]] * len(tmName_list),
            "tmName_char_vec": tmName_char_vec_list,
        }
    )
    # compute similarity
    for index, row in result_df.iterrows():
        sm = SequenceMatcher(
            None, target_tmName_char_vec_list, row["tmName_char_vec"], True, threshold, glyph
        )
        result_df.at[index, "matching_blocks"] = sm.get_matching_blocks()
        result_df.at[index, "similarity"] = sm.ratio()
    result_df = result_df.sort_values(by=["similarity"], ascending=False)
    date = datetime.now().strftime("%m%d")
    time = datetime.now().strftime("%H%M")
    
    result = list(zip(result_df["appl_no"], result_df["similarity"]))
    print(result_df)
    
    return result