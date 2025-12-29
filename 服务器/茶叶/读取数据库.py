#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
读取数据库.py
统一封装所有「只读」场景，供爬虫或其它业务模块 import 调用。
"""
import os
import sys
from typing import List, Dict, Any, Optional, Generator

# 把项目根目录加入 PYTHONPATH，保证无论在哪层执行都能 import
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from database.mongodb import db_manager   # 全局单例
from config.settings import config        # 全局单例
from pymongo import ASCENDING, DESCENDING


# -----------------------------------------------------------------------------
# 对外 API
# -----------------------------------------------------------------------------
def get_company_collection():
    """返回 company 集合对象（只读场景可直接用）"""
    return db_manager.connection_pool.get_collection(config.mongodb.collection)


def find_companies(
    filter_: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, Any]] = None,
    skip: int = 0,
    limit: int = 0,
    sort: Optional[List[tuple]] = None,
) -> List[Dict[str, Any]]:
    """
    一次性读取（适合少量数据）
    :param filter_:  MongoDB 查询条件
    :param projection:  需要返回的字段，例如 {"_id": 0, "name": 1}
    :param skip/limit:  分页
    :param sort:        排序，例如 [("register_capital", DESCENDING)]
    """
    if filter_ is None:
        filter_ = {}
    if sort is None:
        sort = []

    col = get_company_collection()
    cursor = col.find(filter_, projection).sort(sort).skip(skip).limit(limit)
    return list(cursor)


def iter_companies(
    filter_: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, Any]] = None,
    batch_size: int = 2000,
    sort: Optional[List[tuple]] = None,
    deduplicate: bool = True,
) -> Generator[Dict[str, Any], None, None]:
    """
    游标式流式读取（大数据量推荐）
    :param deduplicate: 是否根据id字段去重（默认True）
    """
    if filter_ is None:
        filter_ = {}

    col = get_company_collection()
    cursor = col.find(filter_, projection).batch_size(batch_size).allow_disk_use(True)

    # 只有真正需要排序时才加 sort
    if sort:
        cursor = cursor.sort(sort)

    # 如果启用去重，使用set记录已见过的ID
    seen_ids = set() if deduplicate else None

    for doc in cursor:
        if deduplicate:
            doc_id = doc.get('id')
            if doc_id in seen_ids:
                continue  # 跳过重复的ID
            seen_ids.add(doc_id)
        yield doc


def count_companies(filter_: Optional[Dict[str, Any]] = None) -> int:
    """统计符合 filter 的文档数量"""
    return db_manager.count(config.mongodb.collection, filter_ or {})


def distinct_companies(field: str, filter_: Optional[Dict[str, Any]] = None) -> List[Any]:
    """去重后返回某字段的全部值"""
    col = get_company_collection()
    return col.distinct(field, filter_ or {})


# -----------------------------------------------------------------------------
# CLI 演示
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    total = count_companies()
    print(list(total))
    # 不带任何条件，统计全表
    for idx, com in enumerate(iter_companies(batch_size=1000), 1):
        print(com.get('id'))
        # print(f"{idx:04d}\t{com.get('id')}\t{com.get('company_name')}\t{com.get('reg_capital', '-')}")

    db_manager.close()
