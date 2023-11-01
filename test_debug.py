from datetime import datetime
import time
import pandas as pd
from main import text_search

pinyin = False
glyph = True
constraints_mode = True

df = pd.read_excel("/home/ericaaaaaaa/logoshot/data/0909_sms.xlsx")

test_cases = []
answer_list = []

pass_or_fail = []
caseType_list = []
time_list = []
total_time = 0
sms_time_list = []
total_sms_time = 0
sms_cnt = 0

for index, row in df.iterrows():
    test_cases.append(row['關鍵詞'])
    answer_list.append(row["正確答案"])
    if constraints_mode:
        start_date = datetime.strptime(str(row['target_startTime']), '%Y-%m-%d  %H:%M:%S').strftime('%Y/%m/%d')
        end_date = datetime.strptime(str(row['target_endTime']), '%Y-%m-%d  %H:%M:%S').strftime('%Y/%m/%d')
        print(f"-----------------------start searching {row['關鍵詞']}------------------------")
        results, found, caseType, time, sms_time = text_search(
            glyph=glyph, 
            pinyin=pinyin,
            target_tmNames=row['關鍵詞'],
            correct_ans=row["正確答案"],
            target_startTime=start_date,
            target_endTime=end_date,
            target_color=row["tmark-color-desc"]
        )
    else:
        print(f"-----------------------start searching {row['關鍵詞']}------------------------")
        results, found, caseType, time, sms_time = text_search(
            glyph=glyph, 
            pinyin=pinyin,
            target_tmNames=row['關鍵詞'],
            correct_ans=row["正確答案"],
        )
    pass_or_fail.append(found)
    caseType_list.append(caseType)
    time_list.append(time)
    total_time += time
    sms_time_list.append(sms_time)
    if sms_time != 0:
        sms_cnt += 1
    total_sms_time += sms_time

hit_dict = {'hit 1': 0, 'hit 3': 0, 'hit 5': 0, 'hit 10': 0}
for i in pass_or_fail:
    if i <= 10:
        hit_dict['hit 10'] += 1
    if i <= 5:
        hit_dict['hit 5'] += 1
    if i <= 3:
        hit_dict['hit 3'] += 1
    if i <= 1:
        hit_dict['hit 1'] += 1

hit_dict = {key: values / len(test_cases) for key, values in hit_dict.items()}
accuracy = hit_dict["hit 10"]


test_df = pd.DataFrame({
    "關鍵詞": test_cases,
    "正確答案": answer_list,
    "in top #": pass_or_fail,
    "case type": caseType_list,
    "time used": time_list,
    "sms time": sms_time_list,
})
print(test_df)

date = datetime.now().strftime("%m%d")
time = datetime.now().strftime("%H%M")

if glyph:
    test_df.to_csv(f"/home/ericaaaaaaa/logoshot/results/test_results/glyph_{date}_{time}_c?{constraints_mode}_{accuracy: .3f}.csv", index=False)
else:
    test_df.to_csv(f"/home/ericaaaaaaa/logoshot/results/test_results/pinyin_{date}_{time}_c?{constraints_mode}_{accuracy: .3f}.csv", index=False)

print("hit dict", hit_dict)

print("average time", total_time / test_df.shape[0])
print("average sms time", total_sms_time / sms_cnt)


# search_params = {"metric_type": "L2", "params": {"nprobe": nprobe}}
# results = collection.search(
#         data="海", 
#         anns_field="vector", 
#         param=search_params,
#         limit=1, 
#         output_fields=['vector'], # set the names of the fields you want to retrieve from the search result.
#         consistency_level="Strong"
#     )

