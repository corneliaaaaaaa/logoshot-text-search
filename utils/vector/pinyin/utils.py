from pypinyin import pinyin, lazy_pinyin, Style
from utils.vector.pinyin.maps import *

def to_pinyin(utterance):
    length = len(utterance)
    translated = []
    pinyin_encodings = pinyin(utterance, style=Style.TONE2)
    for i in range(length):
        currPinyin = pinyin_encodings[i][0]
        translated.append(put_tone_to_end(currPinyin))
    return translated

def put_tone_to_end(input_pinyin):
    if len(input_pinyin) is 1:
        return input_pinyin + '1'
    tone_index = 0
    tone = '1'
    for index, character in enumerate(input_pinyin):
        if character in ("1","2","3","4"):
            tone_index = index
            tone = input_pinyin[index]
            break
    if tone_index is 0:
        return input_pinyin + "5"
    return input_pinyin[0:index] + input_pinyin[index+1:] + tone

def get_edit_distance_close_2d_code(a):
    res = 0
    try:
        if (a is None):
            print("Error:pinyin({},{})".format(a.toString()))
            return res
        twoDcode_consonant_a = consonantMap_TwoDCode[a.consonant]
        twoDcode_vowel_a = vowelMap_TwoDCode[a.vowel]
    except:
        raise Exception("Error pinyin {}{}".format(a.toString()))
    return twoDcode_consonant_a, twoDcode_vowel_a
