import pandas as pd
import re
import numpy as np
import math
from ast import literal_eval
from scipy.spatial import distance


def compute_similarity(vec1: list = [], vec2: list = [], glyph: bool = False):
    """
    Compute similarity for glyph or pinyin mode.
    """
    if glyph:
        return 1 - distance.cosine(np.array(vec1), np.array(vec2))
    else:
        return - distance.euclidean(vec1, vec2)
