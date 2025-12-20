#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
茶叶企业数据采集系统 - 主程序入口
支持每小时自动重启，断点续传，超时强制终止
"""

import logging
import sys
import time
import multiprocessing
from datetime import datetime
from database.mongodb import db_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('crawler.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

RESTART_INTERVAL = 3600  # 每小时重启（秒）


def init_database():
    """初始化数据库连接"""
    logger.info("=" * 50)
    logger.info("正在初始化数据库连接...")
    logger.info("=" * 50)
    try:
        if db_manager.test_connection():
            logger.info("✅ 数据库连接成功！")
            return True
        else:
            logger.error("❌ 数据库连接失败！")
            return False
    except Exception as e:
        logger.error(f"❌ 数据库初始化异常: {e}")
        return False


def run_single_round():
    """执行单轮采集任务（最多运行1小时）"""
    from 二级数据接口调用 import run

    logger.info("🔧 启动采集子进程...")
    proc = multiprocessing.Process(target=run, name="CrawlerWorker")
    proc.start()
    proc.join(timeout=RESTART_INTERVAL)

    if proc.is_alive():
        logger.warning("⏳ 采集任务超时（1小时），正在终止子进程...")
        proc.terminate()
        proc.join(timeout=10)
        if proc.is_alive():
            if hasattr(proc, 'kill'):  # Python 3.7+
                proc.kill()
            proc.join()
        logger.info("✅ 子进程已清理")
    else:
        logger.info("✅ 采集子进程正常退出")


def main():
    logger.info("\n" + "=" * 50)
    logger.info("🚀 茶叶企业数据采集系统启动")
    logger.info("=" * 50 + "\n")

    try:
        if not init_database():
            logger.error("数据库初始化失败，程序退出")
            sys.exit(1)

        round_count = 0
        while True:
            round_count += 1
            start_time = time.time()
            start_dt = datetime.now()
            logger.info("\n" + "=" * 50)
            logger.info(f"📊 第 {round_count} 轮采集开始")
            logger.info(f"⏰ 开始时间: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"⏱️  最多运行 {RESTART_INTERVAL // 60} 分钟")
            logger.info("=" * 50 + "\n")

            try:
                run_single_round()
            except KeyboardInterrupt:
                logger.warning("⚠️ 用户中断")
                break
            except Exception as e:
                logger.error(f"❌ 第 {round_count} 轮异常: {e}", exc_info=True)

            duration = time.time() - start_time
            logger.info("\n" + "=" * 50)
            logger.info(f"✅ 第 {round_count} 轮结束，耗时 {duration:.0f} 秒")
            logger.info("🔄 3秒后开始下一轮...")
            logger.info("=" * 50 + "\n")

            time.sleep(3)

        logger.info("✅ 所有任务完成")

    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断程序")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ 主程序崩溃: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            logger.info("正在关闭数据库连接...")
            db_manager.close()
            logger.info("✅ 资源清理完成")
        except Exception as e:
            logger.error(f"资源清理失败: {e}")


if __name__ == "__main__":
    # Windows 兼容性（multiprocessing）
    multiprocessing.freeze_support()
    main()