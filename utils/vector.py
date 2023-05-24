import pandas as pd
import re
import numpy as np
import math
from ast import literal_eval
from scipy.spatial import distance


# TODO: will connect to db in the future
path = "C:/Users/USER/projects/logoshot/logoshot-text-search/data/"
df = pd.read_excel(path + "char_vector.xlsx")
# data preprocess
df["pinyin_vector"] = df["pinyin_vector"].map(literal_eval)
df["glyph_vector"] = df["glyph_vector"].map(literal_eval)


def query_vector(char, glyph=True):
    if glyph is True:
        return df[df["word"] == char]["glyph_vector"].iloc[0]
    else:
        return df[df["word"] == char]["pinyin_vector"].iloc[0]


def compute_similarity(char1, char2, glyph=True):
    vec1 = query_vector(char1, glyph)
    vec2 = query_vector(char2, glyph)

    if glyph is True:
        return 1 - distance.cosine(np.array(vec1), np.array(vec2))
    else:
        return 1 - distance.euclidean(vec1, vec2)
