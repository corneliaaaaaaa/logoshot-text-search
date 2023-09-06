import sys

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

def keyword_preprocess(searchKeywords: str=""):
    """
    Replace multiple white spaces in the keyword to one white space only.
    """
    keyword = re.sub(" +", " ", searchKeywords).strip()

    return keyword

def sum_scores(result1: list=[], result2: list=[], showName: bool=False):
    """
    Sum scores from different results.
    """
    tmName_dict = {}
    score_dict = {}

    # calculate the sums
    for appl_no, tmName, score in result2:
        if showName:
            tmName_dict[appl_no] = tmName_dict.get(appl_no, "") + tmName
        score_dict[appl_no] = score_dict.get(appl_no, 0) + score

    for appl_no, score in result1:
        score_dict[appl_no] = score_dict.get(appl_no, 0) + score

    # create a list of tuples with the sums
    if showName:
        result_list = [(appl_no, tmName_dict.get(appl_no, "unknown"), score_dict.get(appl_no, 0)) for appl_no in score_dict]
    else:
        result_list = [(appl_no, score_dict.get(appl_no, 0)) for appl_no in score_dict]

    return result_list
