import sys

def get_object_size(obj):
    """Recursively calculates the memory used by an object and all nested elements."""
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
    “appl-no”, “CNS-COMPONENTS” in 'source'.
    """
    return (
        hit_item["_source"]["tmark-name"],
        hit_item["_source"]["appl-no"],
        tuple(hit_item["_source"]["CNS_COMPONENTS"]),
    )
