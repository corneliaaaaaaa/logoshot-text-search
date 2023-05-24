import warnings
import csv
import pandas as pd
import re
import numpy as np
import math
from utils.vector.pinyin.pinyin import Pinyin, load_pinyin_to_simplified, load_pinyin_to_traditional
from utils.vector.pinyin.utils import *
from pypinyin import pinyin, lazy_pinyin, Style
import os

maxD = 30000
doubleConsonantsMap = {}
doubleVowelsMap = {}

pinyin_to_simplified = load_pinyin_to_simplified()
pinyin_to_traditional = load_pinyin_to_traditional()

def get_pinyin_vector(utterance1, pinyin=False):
    utterance1 = re.sub(r"[^\u4e00-\u9fa5]", "", utterance1)
    if not pinyin:
        u1 = to_pinyin(utterance1)
    la = []
    for py in u1:
        la.append(Pinyin(py))
    pinyin_vector = []
    for i in range(len(utterance1)):
        apy = la[i]
        if apy is None:
            raise Exception("!Empty Pinyin {},{}".format(la))
        consonant_i, vowel_i = get_edit_distance_close_2d_code(apy)
        if(consonant_i == (99999.0, 99999.0)):
            consonant_i = (0,0)
        pinyin_vector.extend(list(consonant_i))
        pinyin_vector.extend(list(vowel_i))
        pinyin_vector.extend([apy.tone/10])
    return pinyin_vector


def chinese_length_pinyin(targetTM):
    pinyin_vector = get_pinyin(targetTM)
    length = int(len(pinyin_vector) / 5)
    pinyin_vector = pinyin_vector + [0] * (maxD - len(pinyin_vector))
    return length, pinyin_vector

def chinese_length_glyph(targetTM):
    glyph_vector = get_glyph_vector(targetTM)
    length = int(len(glyph_vector) / 517)
    glyph_vector = glyph_vector + [0] * (maxD - len(glyph_vector))
    return length, glyph_vector

def get_glyph_vector(trademarkName):
    """ 算出商標名稱每個字的 components 組成的 list """
    # create component id set
    curr_dir, _ = os.path.split(__file__)
    root_dir, _ = os.path.split(curr_dir)
    DATA_PATH = os.path.join(root_dir, "vector/glyph", "CNS_SUMMARY_TABLE.csv")
    df = pd.read_csv(DATA_PATH)
    df = df[df['TEXT'] != '###']    
    component_id_set = set()
    for index, row in df.iterrows():
        for c in re.split(',|;', row['COMPONENTS']):
            component_id_set.add(int(c))

    # create component mapping dictionary (define the dimension id of the component )
    component_mapping_dict = {}
    for i in range(len(component_id_set)):
        component_mapping_dict[list(component_id_set)[i]] = i

    # compute the list representing glyph vector for the trademark
    targetTM = trademarkName
    targetTMComponentsList = []
    try:
        targetTM = re.sub(r"[^\u4e00-\u9fa5]", "", targetTM)   # 商標名稱只保留中文 TODO: maybe remove
    except:
        pass
    if targetTM:
        for char in targetTM:
            componentArr = np.zeros(len(component_id_set))
            try:
                component = df[df["TEXT"] == char]["COMPONENTS"].values[0]
                for c in re.split(',|;', component):
                    componentArr[component_mapping_dict[int(c)]] += 1
                norm_componentArr = componentArr / np.linalg.norm(componentArr)   # normalize the array
                norm_componentArr = norm_componentArr / math.sqrt(len(targetTM))
                for i in norm_componentArr.tolist():
                    targetTMComponentsList.append(i)
            except:
                pass
    
    # fill in 0s to make the vector of 30000 dimensions
    targetTMComponentsList.extend([0] * (maxD - len(targetTMComponentsList)))

    return targetTMComponentsList