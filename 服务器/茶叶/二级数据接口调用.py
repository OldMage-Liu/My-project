# -*- coding: utf-8 -*-
"""
二级公司详情数据采集脚本（最终版 + 空ID跳过增强）
功能：
  - 从数据库读取公司 ID 列表
  - 调用 API 获取详情
  - 将所有复杂字段（list/dict）转为 JSON 字符串后存入 MongoDB
  - 断点续传 + 日志记录
  - 所有字段在数据库中均为可读字符串，便于查看和导出
  - 自动跳过 id 为空的记录，并记录到 invalid_company_records.log
"""

import time
import logging
import os
import json
import requests

# 自定义模块导入
from 获取令牌 import 令牌
from 读取数据库 import count_companies, iter_companies  # 假设返回生成器
from database.mongodb import db_manager                  # MongoDB 管理器
from config.settings import config                      # 配置文件

# ==================== 配置区 ====================

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("detail_crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 文件路径
CHECKPOINT_FILE = "二级数据.txt"
INVALID_RECORDS_LOG = "invalid_company_records.log"

# MongoDB 目标集合名
DETAIL_COLLECTION = getattr(config.mongodb, 'collection_detail', 'company_details')

# ====== 全局缓存 token ======
_cached_token = None

def refresh_token():
    global _cached_token
    logging.info("🔄 正在刷新访问令牌...")
    _cached_token = 令牌()
    return _cached_token

def get_headers():
    global _cached_token
    if _cached_token is None:
        refresh_token()
    return {
        'authorization': f'Bearer {_cached_token}',
        'accept': 'application/json',
        'content-type': 'application/json;charset=UTF-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'origin': 'https://yunfu-open.jingxiansuo.com',
        'referer': 'https://yunfu-open.jingxiansuo.com/'
    }

# ==================== 核心：将复杂字段转为 JSON 字符串 ====================

def normalize_complex_fields(data: dict) -> dict:
    """
    将指定的 list / dict 类型字段转换为格式化的 JSON 字符串，
    确保在 MongoDB 中以纯文本形式存储，便于查看。
    """
    target_fields = [
        'companyTags',
        'historyNames',
        'products',
        'socialSecurities',
        'businessScope',
        'websiteStates',
        'judiIInformNum',
        'kpNum',
        'licenseNum',
        'honorNum',
        'standardsNum',
        'interlinkMobileNum',
        'interlinkFixedLineNum',
        'interlinkEmailNum',
        'interlinkQqNum',
        'interlinkWechatNum',
        'interlinkFaxNum',
        'interlinkOtherContactNum',
        'interlinkKpNum',
        'linkinNum',
        'maimaiNum',
        'certificatesNum',
        'curInvestmentNum',
        'investmentNum',
        'hisInvestmentNum',
        'ecommerceNum'
    ]

    result = data.copy()
    for field in target_fields:
        if field in result and isinstance(result[field], (dict, list)):
            try:
                result[field] = json.dumps(result[field], ensure_ascii=False, indent=2)
            except Exception as e:
                logging.warning(f"⚠️ 字段 '{field}' 序列化失败，改用 str(): {e}")
                result[field] = str(result[field])
    return result

# ==================== 工具函数 ====================

def load_processed_ids() -> set:
    """加载已处理的 company_id 集合（用于跳过）"""
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        logging.error(f"⚠️ 读取断点文件失败: {e}")
        return set()

def record_success(company_id: str):
    """追加写入成功处理的 ID 到断点文件"""
    try:
        with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
            f.write(f"{company_id}\n")
    except Exception as e:
        logging.error(f"⚠️ 记录成功 ID 失败: {e}")

def save_to_mongo(detail_data: dict) -> bool:
    """将单条公司详情写入 MongoDB（先标准化复杂字段）"""
    if not detail_data or not isinstance(detail_data, dict):
        return False
    try:
        normalized = normalize_complex_fields(detail_data)
        normalized["fetched_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        ok = db_manager.insert_one(data=normalized, collection_name=DETAIL_COLLECTION)
        return ok
    except Exception as e:
        logging.exception(f"MongoDB 写入异常: {e}")
        return False

# ==================== 主逻辑 ====================

def run():
    global _cached_token
    total = count_companies()
    processed_ids = load_processed_ids()
    logging.info(f"📊 共 {total} 条公司数据，已处理 {len(processed_ids)} 条，开始请求...")

    refresh_token()

    for idx, com in enumerate(iter_companies(batch_size=1000), start=1):
        company_id = com.get('id')
        # 检查 ID 是否有效
        if not company_id:
            # 构造无效记录日志
            invalid_record = {
                "index": idx,
                "raw_data": com,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            try:
                with open(INVALID_RECORDS_LOG, "a", encoding="utf-8") as f:
                    f.write(json.dumps(invalid_record, ensure_ascii=False) + "\n")
            except Exception as e:
                logging.error(f"写入无效记录日志失败: {e}")

            logging.warning(f"[{idx}] ⚠️ 跳过空或无效 ID 的记录: {com}")
            continue  # 跳过本次循环

        company_id_str = str(company_id)
        if company_id_str in processed_ids:
            logging.info(f"[{idx}] ✅ 已处理，跳过: {company_id_str}")
            continue

        url = (
            f"https://baize-api-yunfu.jingxiansuo.com/DataService/api/v2/company/detail/"
            f"{company_id_str}?clickPath=advanced-search"
        )

        success = False
        max_retries = 3

        for attempt in range(max_retries):
            try:
                headers = get_headers()
                response = requests.get(url, headers=headers, timeout=15)  # 超时略微放宽

                if response.status_code == 200:
                    try:
                        resp_json = response.json()
                        if resp_json.get("status") != 200 or not resp_json.get("success"):
                            message = resp_json.get("message", "未知错误")
                            logging.warning(f"[{idx}] API 业务错误（尝试 {attempt + 1}/{max_retries}）: {message}")
                            refresh_token()
                            if attempt < max_retries - 1:
                                time.sleep(1)
                            continue

                        detail = resp_json.get("data")
                        if not detail:
                            logging.warning(f"[{idx}] 无 data 字段（尝试 {attempt + 1}/{max_retries}）: {url}")
                            refresh_token()
                            if attempt < max_retries - 1:
                                time.sleep(1)
                            continue

                        if save_to_mongo(detail):
                            logging.info(f"[{idx}] 💾 成功保存详情: {company_id_str}")
                            success = True
                            break
                        else:
                            logging.error(f"[{idx}] ❌ MongoDB 写入失败（尝试 {attempt + 1}/{max_retries}）")
                            refresh_token()
                            if attempt < max_retries - 1:
                                time.sleep(1)
                            continue

                    except json.JSONDecodeError:
                        logging.error(
                            f"[{idx}] ❌ JSON 解析失败（尝试 {attempt + 1}/{max_retries}）: {response.text[:300]}..."
                        )
                        refresh_token()
                        if attempt < max_retries - 1:
                            time.sleep(1)
                        continue

                else:
                    # 检查是否返回 HTML（如 Token 失效跳转登录页）
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type or '缺少令牌' in response.text:
                        logging.warning(f"[{idx}] 检测到鉴权失败页面，强制刷新 Token")
                        refresh_token()
                        if attempt < max_retries - 1:
                            time.sleep(2)
                        continue

                    logging.warning(f"[{idx}] 📡 HTTP {response.status_code}（尝试 {attempt + 1}/{max_retries}）")
                    refresh_token()
                    if attempt < max_retries - 1:
                        time.sleep(1)
                    continue

            except requests.RequestException as e:
                logging.warning(f"[{idx}] ⚠️ 网络异常（尝试 {attempt + 1}/{max_retries}）: {e}")
                refresh_token()
                if attempt < max_retries - 1:
                    time.sleep(1)
                continue

        if success:

            record_success(company_id_str)
        else:
            logging.error(f"[{idx}] ❌ 所有 {max_retries} 次尝试均失败，跳过公司: {company_id_str}")

        time.sleep(1)  # 控制请求频率

    logging.info("✅ 所有任务完成！")

# ==================== 启动入口 ====================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.info("🛑 用户中断程序")
    except Exception as e:
        logging.exception(f"💥 程序崩溃: {e}")