from main import text_search
import os
import time
import pandas as pd
from datetime import datetime

pinyin = False
glyph = True

test_list = []
if pinyin:
    test_list = [
        ("星於及圖", "STARLUX星宇航空及圖(彩色)"),
        ("雄師及圖", "雄獅旅遊LION TRAVEL及圖"),
        ("原之神及圖", "原神及圖"),
        ("東深及圖", "東森購物及圖"),
        ("燦捆電器及圖", "TK燦坤電生活TK3C.com及圖"),
        ("瞎皮", "蝦皮購物 & S DEVICE"),
        ("惺叭客及圖", "星巴克咖啡及圖 (XING BA KE in Chinese characters w/design)"),
        ("路一沙咖啡及圖", "路易莎 LOUISA 及圖"),
        ("常容海", "長榮海運股份有限公司及圖 EVERGREEN MARINE CORPORATION"),
        ("兴噠", "興達海基 SINGDA MARINE STRUCTURE"),
        ("艾爾噠", "ELTA HD 愛爾達電視"),
        ("地五人", "第五人格"),
    ]
else:
    test_list = [
        ("師旅遊及圖", "雄獅旅遊LION TRAVEL及圖"),
        ("麥噹嘮公司", "麥當勞 (McDonald's in Chinese)"),
        ("凍森及圖", "東森購物及圖"),
        ("璨伸電器及圖", "TK燦坤電生活TK3C.com及圖"),
        ("和溙險及圖", "和泰產險HOTAI INSURANCE及圖"),
        ("猩吧各及圖", "星巴克咖啡及圖 (XING BA KE in Chinese characters w/design)"),
        ("潞易砂咖啡及圖", "路易莎 LOUISA 及圖"),
        ("待力之屋及圖", "特力屋及圖"),
        ("副倸公司", "Ennostar富采控股設計字"),
        ("日本樂夭", "樂天 RAKUTEN 及圖"),
    ]

file_str = "glyph"
if pinyin:
    file_str = "pinyin"
date = datetime.now().strftime("%m%d")
time = datetime.now().strftime("%H%M")


with open(f"./logoshot/results/test_results/{file_str}_{date}_{time}_check_go_milvus.txt", "w") as file:
    file.write("關鍵詞, 正確答案 \n")
    file.write("回傳資料的前 10 名 \n")
    for test in test_list:
        results = text_search(
            glyph=glyph,
            pinyin=pinyin,
            target_tmNames=test[0],
            correct_ans=test[1],
            # target_startTime="2016/11/11",
            # target_endTime="2016/11/14",
            # target_color="彩色"
        )
        file.write(f"{test[0]}, {test[1]} \n")
        for r in results[:10]:
            file.write(f"{str(r)} \n")
        file.write("\n")
