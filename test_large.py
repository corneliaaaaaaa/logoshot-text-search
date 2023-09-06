from main import text_search
import pandas as pd
from datetime import datetime
import time

pinyin = True
glyph = False
constraints_mode = False
same_length = False

df = pd.read_excel("/home/ericaaaaaaa/logoshot/data/0716_test_data.xlsx")

test_cases = []
answer_list = []
pass_or_fail = []
caseType_list = []
time_list = []
length_mode = []
total_time = 0

if glyph:
    if same_length:
        length_mode = ["形近(長度同)"] 
    else:
        length_mode = ["形近(長度同)", "形近(長度不同)"]
else:
    if same_length:
        length_mode = ["音近(長度同)"] 
    else:
        length_mode = ["音近(長度同)", "音近(長度不同)"]

for l in length_mode:
    for index, row in df[30:45].iterrows():
        test_cases.append(row[l])
        answer_list.append(row["正確答案"])
        print(f"-----------------------start searching {row[l]}------------------------")
        if constraints_mode:
            start_date = datetime.strptime(str(row['target_startTime']), '%Y-%m-%d  %H:%M:%S').strftime('%Y/%m/%d')
            end_date = datetime.strptime(str(row['target_endTime']), '%Y-%m-%d  %H:%M:%S').strftime('%Y/%m/%d')
            results, found, caseType, time = text_search(
                glyph=glyph, 
                pinyin=pinyin,
                target_tmNames=row[l],
                correct_ans=row["正確答案"],
                target_startTime=start_date,
                target_endTime=end_date,
                target_color=row["tmark-color-desc"]
            )
        else:
            results, found, caseType, time = text_search(
                glyph=glyph, 
                pinyin=pinyin,
                target_tmNames=row[l],
                correct_ans=row["正確答案"],
            )
        pass_or_fail.append(found)
        caseType_list.append(caseType)
        time_list.append(time)
        total_time += time

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
})
print(test_df)

date = datetime.now().strftime("%m%d")
time = datetime.now().strftime("%H%M")

if glyph:
    test_df.to_csv(f"/home/ericaaaaaaa/logoshot/results/test_results/glyph_{date}_{time}_c?{constraints_mode}_{accuracy: .3f}.csv", index=False)
else:
    test_df.to_csv(f"/home/ericaaaaaaa/logoshot/results/test_results/pinyin_{date}_{time}_c?{constraints_mode}_{accuracy: .3f}.csv", index=False)
    
print("average time", total_time / test_df.shape[0])
print("hit dict", hit_dict)