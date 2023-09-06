import warnings
import csv
import pandas as pd
import re
import numpy as np
import math
import os
from pypinyin import pinyin, lazy_pinyin, Style
from utils.vector.pinyin.pinyin import Pinyin, load_pinyin_to_simplified, load_pinyin_to_traditional
from utils.vector.pinyin.utils import *

glyph_max_name_length = 7
pinyinDim = 5
glyphDim = 517
maxDimPinyin = 300
maxDimGlyph = 517 * glyph_max_name_length

doubleConsonantsMap = {}
doubleVowelsMap = {}

pinyin_to_simplified = load_pinyin_to_simplified()
pinyin_to_traditional = load_pinyin_to_traditional()

curr_dir, _ = os.path.split(__file__)
root_dir, _ = os.path.split(curr_dir)
DATA_PATH = os.path.join(root_dir, "vector/glyph", "CNS_SUMMARY_TABLE.csv")
df = pd.read_csv(DATA_PATH)
df = df[df['TEXT'] != '###']    
component_id_set = set()
for index, row in df.iterrows():
    for c in re.split(',|;', row['COMPONENTS']):
        component_id_set.add(int(c))
print("load CNS summary table done")

"""
def get_pinyin_vector(utterance1, pinyin=False, unit = False):
    # cosine similarity
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
        word_list = []
        word_list.extend(list(consonant_i))
        word_list.extend(list(vowel_i))
        word_list.extend([apy.tone/10])
        word_list = word_list / np.linalg.norm(word_list)  
        word_list = word_list / math.sqrt(len(utterance1))
        pinyin_vector.extend(word_list)
    return pinyin_vector
"""
def chinese_length_pinyin(targetTM: str=""):
    """
    Get both the length and pinyin vector of the trademark name.
    The function adds 0 values to make the dimension of each trademark's vector the same.
    """
    pinyin_vector = get_pinyin_vector(targetTM)
    length = int(len(pinyin_vector) / pinyinDim)
    pinyin_vector = pinyin_vector + [0] * (maxDimPinyin - len(pinyin_vector))
    return length, pinyin_vector

def chinese_length_glyph(targetTM: str=""):
    """
    Get both the length and glyph vector of the trademark name.
    The function adds 0 values to make the dimension of each trademark's vector the same.
    """
    glyph_vector = get_glyph_vector(targetTM)
    length = int(len(glyph_vector) / glyphDim)
    glyph_vector = glyph_vector + [0] * (maxDimGlyph - len(glyph_vector))
    return length, glyph_vector[:maxDimGlyph]

def unit_pinyin(targetTM: str=""):
    """
    Get the pinyin vector of the trademark name.
    """
    d = {}
    word = re.sub(r"[^\u4e00-\u9fa5]", "", targetTM)
    for w in word:
        pinyin_vector = get_pinyin_vector(w)
        d[w] = pinyin_vector
    return d

def unit_glyph(targetTM: str=""):
    """
    Get the glyph vector of the trademark name.
    """
    d = {}
    word = re.sub(r"[^\u4e00-\u9fa5]", "", targetTM)
    for w in word:
        glyph_vector = get_glyph_vector(w)
        d[w] = glyph_vector
    return d

def get_pinyin_vector(utterance1: str="", pinyin: bool=False):
    """
    Get the pinyin vector of the trademark name.
    """
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
        word_list = []
        word_list.extend(list(consonant_i))
        word_list.extend(list(vowel_i))
        word_list.extend([apy.tone/10])
        pinyin_vector.extend(word_list)
    return pinyin_vector

def get_glyph_vector(
    trademarkName: str="", 
    component_id_set: set=component_id_set, 
    size: int=glyph_max_name_length
):
    """
    Get the glyph vector of the trademark name.
    """
    # create component mapping dictionary (define the dimension id of the component )
    component_mapping_dict = {}
    for i in range(len(component_id_set)):
        component_mapping_dict[list(component_id_set)[i]] = i

    # compute the list representing glyph vector for the trademark
    targetTM = trademarkName
    targetTMComponentsList = []
    try:
        targetTM = re.sub(r"[^\u4e00-\u9fa5]", "", targetTM)   # 商標名稱只保留中文
        if(len(targetTM) > size):
            targetTM = targetTM[:size]
    except:
        pass

    if targetTM:
        for char in targetTM:
            componentArr = np.zeros(len(component_id_set))
            try:
                # get the component list of the character
                component = df[df["TEXT"] == char]["COMPONENTS"].values[0]
                # turn the component list into the binary vector
                for c in re.split(',|;', component):
                    componentArr[component_mapping_dict[int(c)]] += 1
                norm_componentArr = componentArr / np.linalg.norm(componentArr)   # normalize the array
                norm_componentArr = norm_componentArr / math.sqrt(len(targetTM))
                for i in norm_componentArr.tolist():
                    targetTMComponentsList.append(i)
            except:
                pass

    return targetTMComponentsList