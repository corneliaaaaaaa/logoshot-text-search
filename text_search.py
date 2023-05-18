import difflib_for_comparing_similar_strings as difflib
import pandas as pd


def text_search(
    trademark_name_list: list,
    target_trademark_name: str,
    threshold: float = 0.5,
    glyph: bool = True,
):
    if glyph:
        print(
            f"glyph similarity, keyword = {target_trademark_name}, threshold = {threshold}"
        )
    else:
        print(
            f"pinyin similarity, keyword = {target_trademark_name}, threshold = {threshold}"
        )

    result = pd.DataFrame(
        {
            "trademark_name": trademark_name_list,
            "similarity": [0.0] * len(trademark_name_list),
            "matching_blocks": [[]] * len(trademark_name_list),
        }
    )
    for index, row in result.iterrows():
        sm = difflib.SequenceMatcher(
            None, target_trademark_name, row["trademark_name"], True, threshold, glyph
        )
        result.at[index, "matching_blocks"] = sm.get_matching_blocks()
        result.at[index, "similarity"] = sm.ratio()
    result = result.sort_values(by=["similarity"], ascending=False)
    print(result)

    return result


trademark_name_list = [
    "養肌廠",
    "養雞場的人",
    "養雞廠廠長",
    "末位者",
    "養肌力廠",
    "唐揚雞廠",
    "癢癢肌廠",
    "海底撈",
    "海底撈一波",
    "嗨小弟撈撈城",
    "排到後面",
    "海撈這一家",
    "嗨迪",
    "海底撈撈",
    "鼎泰豐",
    "頂著龍捲風",
    "不是鼎泰豐",
    "我是鼎泰瘋",
    "鼎著秦豐",
    "鼎鼎大豐",
    "你也到後面",
]

threshold = 0.5
glyph = True
target_trademark_name = "海底撈"

text_search(trademark_name_list, target_trademark_name, threshold, glyph)
text_search(trademark_name_list, target_trademark_name, threshold, not glyph)
