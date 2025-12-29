import logging
import sys
from database.mongodb import db_manager
from baidu_spider import run_main

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def main():
    """主函数"""
    print("🚀 启动爬虫框架")

    # 测试数据库连接
    try:
        if db_manager.test_connection():
            print("✅ 数据库连接成功")
        else:
            print("❌ 数据库连接失败")
    except Exception as e:
        print(f"❌ 数据库连接错误: {e}")
    try:

        run_main()
    except  Exception as e:
        print('爬虫失败')



if __name__ == "__main__":
    main()