# from main import text_search
import os
import numpy as np
from utils.vector.vector import unit_glyph, unit_pinyin, get_pinyin_vector, get_glyph_vector
from utils.similarity import compute_similarity

# pinyin = False
# glyph = True

# es_results_id = text_search(
#     glyph=glyph,
#     pinyin=pinyin,
#     # target_tmNames="麥檔老",
#     target_tmNames="閏發顯食",  # "麥噹嘮公司 (McDonald's in Chinese)",
#     # correct_ans="麥當勞 (McDonald's in Chinese)",
#     # target_startTime="2016/11/11",
#     # target_endTime="2016/11/14",
#     target_color="彩色"
# )
# print('es', es_results_id[:10])
# print("found: ", found)
# print("caseType: ", caseType)
# print("time: ", time)
# # vec1 = unit_glyph('蝦皮購物')
# # vec2 = unit_glyph('瑕批購物')
# vec1 = get_pinyin_vector('大閏發顯食集')
# vec2 = get_pinyin_vector('大潤發鮮食集')

pinyin = False
glyph = True
if glyph:
    vec1 = get_glyph_vector('天閏撥蘚食集')
    vec2 = get_glyph_vector('大潤發鮮食集')
    # vec1 = get_glyph_vector('鮮')
    # vec2 = get_glyph_vector('蘚')
    print(vec1)
    print(vec2)

    c = compute_similarity(vec1, vec2, True)
    print(c)
else:
    # vec1 = get_pinyin_vector('先')
    # vec2 = get_pinyin_vector('顯')
    vec1 = get_pinyin_vector('大閏發顯食集')
    vec2 = get_pinyin_vector('大潤發鮮食集')
    print(vec1)
    print(vec2)

    # # 瞎皮、瑕批: 0.8585786437626906
    c = compute_similarity(vec1, vec2, False)
    # c = compute_similarity(vec1['大閏發顯食集'], vec2['大潤發鮮食集'], True)
    # # c = compute_similarity(vec1['蝦'] / np.linalg.norm(vec1['蝦']), vec2['瑕'] / np.linalg.norm(vec2['瑕']), True)
    print(c)
