from main import text_search
import pandas as pd

# test cases for demo & efficiency
# 嚴格搜尋
# text_search(glyph=False, pinyin=False, target_tmNames="海底勞")

# 形近/音近搜尋
# milvus score > threshold
# text_search(glyph=False, pinyin=True, target_tmNames="海底勞", target_color="墨色") # 有其他條件
# text_search(glyph=False, pinyin=True, target_tmNames="海底勞") # 無其他條件

# no milvus score > threshold TODO: 還沒找到測資，除非要調 threshold
# text_search(glyph=False, pinyin=True, target_tmNames="鄭安芸",  target_applicant="公司") # 有其他條件
# text_search(glyph=False, pinyin=True, target_tmNames="栗綺又長高了") # 無其他條件

# test cases for accuracy
test_cases = [
    "星宇", "興與航空", "星星航空公司", "星宇宙航空", "瑆宇",
    "拓元售票", "橢圓售票", "拓展元寶", "拓沅", "拓院公司",
    "雄鷹旅遊社", "兇獅", "高雄獅旅行團", "雄雌旅遊", "雄獅",
    "賣噹噹勞", "麥當勞工局", "小麥當賣", "麥當當", "麥當水澇",
    "元氣神", "原來神", "草原之神", "圓神", "怨神", "原來是神",
    "東森", "動森",	"童森購物", "東東愛購物", "東邊森林購物", "凍森森溝物",
    "燦坤", "燦坤電器", "慘坤",	"慘睏生活",	"璨申",	"粲伸電公司",
    "小米粒", "小迷妹",	"小大米", "消米", "米", "小小瞇",
    "和泰", "合泰", "合秦", "和秦產險公司",	"泰山產險",	"喝太多產業",
    "蝦米購物網", "瞎皮", "蝦皮", "瞎眼皮購物",	"蝦皮應用程式",	"嚇屁",
    "八方雲集水餃",	"八十八方雲集",	"霸氣一方雲集",	"霸放暈朵季", "八方", "拔放運氣",
    "星雲巴客",	"流星巴客",	"星九克", "辛巴", "星巴克咖啡",
    "麻古",	"麻股茶", "痲古茶訪", "麻古飲料", 
    "露易莎", "路伊沙",	"路易沙咖啡", "如意沙",
    "特力屋超市", "特給力",	"特刃屋", "特別給力屋",
    "好多市", "好市多多", "好沛多", "好事多磨",
    "高端",	"搞湍",	"告短衣苗",	"高端醫療",
    "長榮海", "長榮", "長榮航海運",	"暢榮嗨運",
    "傳說", "打傳說對決", "傳言兌對決",	"傳說遊戲", "喘碩隊倔",
    "莫德納疫苗", "默德納",	"莫德納稅",	"摸的鈉",
    "興達公司", "星達", "興噠", 
    "富采公司",	"富彩公司", "福彩", "富有采",
    "中央衛生局", "中衛醫療", "中尉", "仲衛",
    "大潤發賣場", "周潤發",	"打潤髮", "去買大潤發",
    "捷安特腳踏車",	"捷安特公司", "節鞍忑",	"睫按特別",
    "小三美妝",	"小三美", "小三", "小山美日",
    "中天", "中天新聞網", "仲天心問", "忠天薪聞", "種田",
    "桂冠食品",	"桂花冠", "規官", "圭冠",
    "味道全聾", "味全龍隊", "味全隆", "未全籠",
    "地卡農", "迪卡農",	"迪克卡農",	"迪卡",
    "樂天世界", "樂天百貨公司",	"快樂一天",	"樂天派", "樂舔",
    "柏克萊", "傅各萊",	"博客來書店", "博客",
    "愛爾達", "愛爾達網路",	"愛爾韃", "矮二大典施",
    "五人格", "地五人格", "第八人格", "地武個人格",	"五個人格",
    "一芳水果茶", "一芳飲料店",	"在水一方",	"一方",	"一杯芳"
    "普悠瑪列車", "譜攸馬", "樸悠瑪", "圃優馬航班",
    "國巨匠", "國超巨",	"摑拒",	"國巨企業",	"國劇",
    "胖老爹美式炸雞", "胖爹爹",	"旁烙跌", "胖胖老爹", "胖佬爹",
    "愛奇藝公司", "艾歧異", "曖倚藝",
    "威力采", "威力彩券", "維力猜",
    "東方芝", "動支", "日本東芝", "凍之",
    "富邦",	"富邦罕見", "富邦悍將集團", "福綁旱將",
    "宏碁電腦",	"宏基",	"鴻基"
]
answer_set = [
    "星宇航空", "拓元", "雄獅旅遊", "麥當勞", "原神", "東森購物", "燦坤電生活", "小米", "和泰產險", "蝦皮購物",
    "八方雲集", "星巴克", "麻古茶坊", "路易莎", "特力屋", "好市多", "高端疫苗", "長榮海運", "傳說對決", "莫德納",
    "興達", "富采", "中衛", "大潤發", "捷安特", "小三美日", "中天新聞", "桂冠", "味全龍", "迪卡儂",
    "樂天", "博客來", "愛爾達電視", "第五人格", "一芳", "普悠瑪", "國巨", "胖老爹", "愛奇藝", "威力彩",
    "東芝", "富邦悍將", "宏碁"
]
answer_cnt = [
    5, 5, 5, 5, 6, 6, 6, 6, 6, 6,
    6, 5, 4, 4, 4, 4, 4, 4, 5, 4,
    3, 4, 4, 4, 4, 4, 5, 4, 4, 4,
    5, 4, 4, 5, 5, 4, 5, 5, 3, 3,
    4, 4, 2 #TODO
]
answer_list = []
pass_or_fail = [False] * len(test_cases)
caseType_list = [""] * len(test_cases)
time_list = [0] * len(test_cases)

for i in range(len(answer_set)):
    tmp = [answer_set[i]] * answer_cnt[i]
    answer_list.extend(tmp)

print(answer_list)
print(len(answer_list))
print(len(test_cases))

for i in range(len(test_cases)):
    results, in_top_ten, caseType, time = text_search(glyph=False, pinyin=True, target_tmNames=test_cases[i])
    pass_or_fail.append(in_top_ten)
    caseType_list.append(caseType)
    time_list.append(time)

accuracy = sum(pass_or_fail) / len(test_cases)
test_df = pd.DataFrame({
    "關鍵詞": test_cases,
    "正確答案": answer_list,
    "in top ten": pass_or_fail,
    "case type": caseType_list,
    "time used": time_list,
})

test_df.to_csv(f"/home/ericaaaaaaa/logoshot/data/test_results/v1_pinyin_0611_{accuracy: .3f}.csv")
