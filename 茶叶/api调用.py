# -*- coding: utf-8 -*-
"""
API 数据采集模块（独立运行或被主程序调用）
"""

import time
import random
import requests
import os
import json
from 获取令牌 import 令牌
from database.mongodb import db_manager
from config.settings import config

URL = 'https://baize-api-yunfu.jingxiansuo.com/DataService/api/v1/company/list/multiple'
BREAKPOINT_FILE = 'last_page.txt'

BATCH_SIZE = 20


def load_last_page() -> int:
    """
    读取上次完成的页码，返回下一页开始位置
    """
    if not os.path.exists(BREAKPOINT_FILE):
        return 1
    try:
        with open(BREAKPOINT_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            last_completed = int(content) if content else 0
            # 返回下一页，避免重复获取
            return last_completed + 1
    except Exception as e:
        print(f"⚠️ 读取断点文件失败: {e}")
        return 1


def save_last_page(page: int):
    try:
        with open(BREAKPOINT_FILE, 'w', encoding='utf-8') as f:
            f.write(str(page))
    except Exception as e:
        print(f"⚠️ 保存断点失败: {e}")


def fresh_headers() -> dict:
    return {
        'authorization': f'Bearer {令牌()}',
        'accept': 'application/json',
        'content-type': 'application/json;charset=UTF-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'origin': 'https://yunfu-open.jingxiansuo.com',
        'referer': 'https://yunfu-open.jingxiansuo.com/'
    }


def req_with_retry(data: dict, max_retry: int = 3) -> requests.Response:
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.post(URL, json=data, headers=fresh_headers(), timeout=15)
            if resp.status_code == 200:
                return resp
        except Exception as e:
            pass
        time.sleep((2 ** attempt) + random.random())
    raise RuntimeError('重试耗尽仍失败')


def save_to_mongo(batch):
    if not batch:
        return
    collection_name = config.mongodb.collection
    ok = db_manager.insert_many(data_list=batch, collection_name=collection_name)
    if ok:
        print(f"✓ MongoDB 写入 {len(batch)} 条")
    else:
        print("✗ 写入 MongoDB 失败")


def run():
    """执行单次完整采集任务（从断点开始）"""
    condition_obj = {
    "must": [
        {"companyName": [{"in": ["新能源"]}]},
        {"operateState": [{"eq": ["存续", "在业"]}]},
        {"manageScope": [{"in": ["新能源", "经营", "技术", "咨询", "客户", "提供", "办公", "劳务", "位于", "项目", "劳动", "设备", "工程", "管理", "期待", "公室", "办公室", "信息", "发展", "企业", "零售", "设计", "店铺", "工商", "用品", "行业", "活动", "批准", "开发", "咨询服务", "留言"]}]},
        {"businessLocation": [{"in": ["广东省"]}]}
    ]
}

    start_page = load_last_page()
    data_batch = []

    # 最多采集到第 4000 页（可根据需要调整）
    for page in range(start_page, 4000):
        try:
            data = {
                "condition": json.dumps(condition_obj, ensure_ascii=False),
                "leadsFilter": "unlock",
                "pageIndex": page,
                "pageSize": 20,
                "clickPath": "advanced-search"
            }

            resp = req_with_retry(data)
            resp_json = resp.json()

            # 安全提取 records
            records = []
            if isinstance(resp_json, dict):
                data_field = resp_json.get('data')
                if isinstance(data_field, dict):
                    records = data_field.get('records', [])
            if not records:
                print(f"ℹ️ 第 {page} 页无数据，可能已到底")
                break

            formatted = []
            for d in records:
                try:
                    item = {
                        "id": d["id"],
                        "company_name": d["companyName"],
                        "legal_person": d["juridicalPerson"],
                        "reg_capital": d["registeredCapital"],
                        "establish_time": d["establishTime"],
                        "address": d["address"],
                        "website": [u for u in d.get("website", []) if u],
                        "contact_cnt": d["contactNum"],
                        "status": d["businessStatus"],
                        "products": d["products"],
                        "source": "API",
                        "page": page
                    }
                    formatted.append(item)
                except KeyError as ke:
                    print(f"⚠️ 第 {page} 页记录缺少字段 {ke}，跳过")

            data_batch.extend(formatted)

            if len(data_batch) >= BATCH_SIZE:
                save_to_mongo(data_batch)
                data_batch = []

            # 数据成功处理后才保存页码（表示此页已完成）
            save_last_page(page)

        except Exception as e:
            print(f"拉取失败：{page} - {e}")
            continue

    # 最后一批写入
    if data_batch:
        save_to_mongo(data_batch)

    print("✅ 本轮采集任务完成")
