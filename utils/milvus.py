from utils.vector.vector import chinese_length_pinyin, chinese_length_glyph, get_pinyin_vector, get_glyph_vector
from pymilvus import connections, Collection

def connect_to_milvus():
    try:
        connections.connect(
            alias="default",
            host='trueint.lu.im.ntu.edu.tw',
            port='19530'
        )
        print('connection success')
    except:
        print('connection failed')

def get_collection(name):
    collection = Collection(name)
    collection.load()
    return collection

def search(nprobe=100, target='', collection=Collection):
    search_params = {"metric_type": "IP", "params": {"nprobe": nprobe}}
    length, pinyin_vector = chinese_length_pinyin(target)
    results = collection.search(
            data=[pinyin_vector], 
            anns_field="vector", 
            param=search_params,
            limit=10000, 
            expr=f"length == {length}",
            output_fields=['appl_no'], # set the names of the fields you want to retrieve from the search result.
            consistency_level="Strong"
        )
    final = [(i, j) for i, j in zip(results[0].ids, results[0].distances)]
    return final