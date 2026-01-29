# make_shards.py
# -*- coding: utf-8 -*-
import os, json, ijson

# 目录/文件名（按你的实际情况）
POISONED_JSONL = 'poisoned_data.jsonl'   # 570MB JSONL
CLEAN_JSON     = 'clean_data.json'       # 545MB JSON Array
OUT_DIR_POI    = 'poisoned_shards'       # 输出每条一个 .json
OUT_DIR_CLEAN  = 'clean_shards'
INDEX_POI      = 'poisoned_index.jsonl'  # 只存 id+question，便于前端检索（很小）
INDEX_CLEAN    = 'clean_index.jsonl'

os.makedirs(OUT_DIR_POI, exist_ok=True)
os.makedirs(OUT_DIR_CLEAN, exist_ok=True)

# --- 1) 处理 poisoned JSONL：逐行读，不占内存 ---
with open(POISONED_JSONL, 'r', encoding='utf-8') as fin, \
     open(INDEX_POI, 'w', encoding='utf-8') as idx:
    for i, line in enumerate(fin, 1):
        line = line.strip()
        if not line: continue
        obj = json.loads(line)
        _id = obj.get('id')
        if not _id: continue
        # 写切片
        with open(os.path.join(OUT_DIR_POI, f'{_id}.json'), 'w', encoding='utf-8') as fout:
            json.dump(obj, fout, ensure_ascii=False)
        # 写索引（id + question 即可）
        idx.write(json.dumps({'id': _id, 'question': obj.get('question','')}, ensure_ascii=False) + '\n')
        if i % 1000 == 0:
            print('poisoned split:', i)

# --- 2) 处理 clean JSON（数组）：用 ijson 流式解析 ---
with open(CLEAN_JSON, 'rb') as fin, \
     open(INDEX_CLEAN, 'w', encoding='utf-8') as idx:
    # ijson.items 遍历最外层数组的每个对象
    for i, obj in enumerate(ijson.items(fin, 'item'), 1):
        _id = obj.get('id')
        if not _id: continue
        with open(os.path.join(OUT_DIR_CLEAN, f'{_id}.json'), 'w', encoding='utf-8') as fout:
            json.dump(obj, fout, ensure_ascii=False)
        idx.write(json.dumps({'id': _id, 'question': obj.get('question','')}, ensure_ascii=False) + '\n')
        if i % 1000 == 0:
            print('clean split:', i)

print('DONE.')
