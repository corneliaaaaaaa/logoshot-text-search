from main import text_search
import os

pinyin = False
glyph = True

results, a, b, c, d = text_search(
    glyph=glyph,
    pinyin=pinyin,
    target_tmNames="麥噹嘮公司 (McDonald's in Chinese)",
    correct_ans="麥當勞 (McDonald's in Chinese)",
    # target_startTime="2016/11/11",
    # target_endTime="2016/11/14",
    # target_color="彩色"
)

# print("found: ", found)
# print("caseType: ", caseType)
# print("time: ", time)
