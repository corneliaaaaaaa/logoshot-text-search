import pandas as pd
import re
import numpy as np
import math
from ast import literal_eval
from scipy.spatial import distance
from utils.vector.vector import *

def get_vector(char, glyph=True):
    if glyph is True:
        return get_glyph_vector(char)
    else:
        return get_pinyin_vector(char)

def compute_similarity(char1, char2, glyph=True):
    vec1 = get_vector(char1, glyph)
    vec2 = get_vector(char2, glyph)
    print("comp", len(vec1), len(vec2))

    if glyph is True:
        return 1 - distance.cosine(np.array(vec1), np.array(vec2))
    else:
        return 1 - distance.euclidean(vec1, vec2)
