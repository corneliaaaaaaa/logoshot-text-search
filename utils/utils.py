import sys
import regex as re


def get_object_size(obj):
    """
    Recursively calculates the memory used by an object and all nested elements.
    """
    size = sys.getsizeof(obj)

    if size is None:
        return 0
    elif isinstance(obj, dict):
        size += sum(
            get_object_size(key) + get_object_size(value) for key, value in obj.items()
        )
    elif isinstance(obj, (list, tuple, set)):
        size += sum(get_object_size(item) for item in obj)
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        size += sum(get_object_size(item) for item in obj)

    return size


def transform_es_return_format(hit_item):
    """
    Turn the original output data format to a more simple one, which only includes "tmark-name",
    "appl-no", "CNS-COMPONENTS" in "_source".
    """
    return (
        hit_item["_source"]["tmark-name"],
        hit_item["_source"]["appl-no"],
        tuple(hit_item["_source"]["CNS_COMPONENTS"]),
    )


def keyword_preprocess(searchKeywords: str = "", strict: bool = True):
    """
    Preprocessing for keywords, which indludes:
    - Replace multiple white spaces in the keyword to one white space only.
    - Extract Chinese, English part from the keyword. (if it's not strict search)

    ===
    param:
    - strict: if the search is strict search
    """
    keyword = re.sub(" +", " ", searchKeywords).strip()

    if strict:
        return keyword, ""
    else:
        chinese_pattern = r'[\u4e00-\u9fa5]+'
        english_pattern = r'[A-Za-z0-9,.\-\'&*:]+'

        # Use re.findall to extract substrings for each language
        chinese_matches = re.findall(chinese_pattern, keyword)
        english_matches = re.findall(english_pattern, keyword)

        # Join the matched substrings to get the desired output
        chinese_text = ' '.join(chinese_matches)
        english_text = ' '.join(english_matches)

        return chinese_text


def sum_scores(result1: list = [], result2: list = [], showName: bool = False):
    """
    Sum scores from different results.
    """
    tmName_dict = {}
    score_dict = {}

    # calculate the sums
    for appl_no, tmName, score in result2:
        if showName:
            tmName_dict[appl_no] = tmName_dict.get(appl_no, "") + tmName
        else:
            score_dict[appl_no] = score_dict.get(appl_no, 0) + score

    for appl_no, score in result1:
        score_dict[appl_no] = score_dict.get(appl_no, 0) + score

    # create a list of tuples with the sums
    if showName:
        result_list = [
            (appl_no, tmName_dict.get(appl_no, "unknown"), score_dict.get(appl_no, 0))
            for appl_no in score_dict
        ]
    else:
        result_list = [(appl_no, score_dict.get(appl_no, 0))
                       for appl_no in score_dict]

    return result_list


def process_results(results):
    """
    Sort results by score and filter out the tuples with the same appl_no as others
    but has a lower score.
    """
    highest_scores = {}

    # iterate through the list and update the highest score for each appl_no
    for appl_no, score in results:
        if appl_no not in highest_scores or score > highest_scores[appl_no]:
            highest_scores[appl_no] = score

    # use a list comprehension to filter out tuples with lower scores
    results = [(appl_no, score)
               for appl_no, score in results if score == highest_scores[appl_no]]

    # sort results
    results = sorted(results, key=lambda x: x[-1], reverse=True)

    return results
