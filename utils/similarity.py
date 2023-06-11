import pandas as pd
import re
import numpy as np
import math
from ast import literal_eval
from scipy.spatial import distance

def compute_similarity(vec1, vec2, glyph=False):
    try:
        return 1 - distance.cosine(np.array(vec1), np.array(vec2))
    except:
        return 0
    