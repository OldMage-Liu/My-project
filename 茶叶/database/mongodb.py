import logging
import threading
import time
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError, ServerSelectionTimeoutError
from pymongo.collection import Collection

from config.settings import config

logger = logging.getLogger(__name__)

class MongoDBConnectionPool:
    """MongoDB连接池管理器 - 修复版本"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        """初始化连接池"""
        self.client: Optional[MongoClient] = None
        self._connect()

    def _get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        return {
            'host': config.mongodb.connection_string,
            'maxPoolSize': config.mongodb.max_pool_size,
            'socketTimeoutMS': config.mongodb.socket_timeout_ms,
            'connectTimeoutMS': config.mongodb.connect_timeout_ms,
            'serverSelectionTimeoutMS': config.mongodb.server_selection_timeout_ms,
            'retryWrites': True,
            'retryReads': True,
        }

    def _connect(self):
        """连接到 MongoDB"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                connection_params = self._get_connection_params()

                # 安全地记录连接字符串（隐藏密码）
                safe_conn_str = config.mongodb.connection_string
                if "@" in safe_conn_str:
                    safe_conn_str = safe_conn_str.split('@')[0] + "@***@" + safe_conn_str.split('@')[1]

                logger.info(f"正在连接 MongoDB ({attempt + 1}/{max_retries})")
                logger.info(f"数据库: {config.mongodb.database}")
                logger.debug(f"连接字符串: {safe_conn_str}")

                self.client = MongoClient(**connection_params)

                # 测试连接
                self.client.admin.command('ping')

                logger.info("✅ 成功连接到 MongoDB")
                return

            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                logger.warning(f"连接尝试 {attempt + 1}/{max_retries} 失败: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"❌ MongoDB 连接失败，已重试 {max_retries} 次")
                    raise
            except Exception as e:
                logger.error(f"❌ 连接 MongoDB 时发生错误: {e}")
                raise

    def get_database(self, database_name: str = None) -> Any:
        """获取数据库实例"""
        if not self.is_connected():
            self._reconnect()

        db_name = database_name or config.mongodb.database
        return self.client[db_name]

    def get_collection(self, collection_name: str, database_name: str = None) -> Collection:
        """获取指定集合实例"""
        db = self.get_database(database_name)
        return db[collection_name]

    def is_connected(self) -> bool:
        """检查连接状态"""
        try:
            if self.client:
                self.client.admin.command('ping')
                return True
            return False
        except PyMongoError:
            return False

    def _reconnect(self):
        """重新连接"""
        logger.warning("检测到连接断开，尝试重新连接...")
        try:
            if self.client:
                self.client.close()
        except:
            pass
        self._connect()

    def close(self):
        """关闭连接池"""
        if self.client:
            self.client.close()
            logger.info("MongoDB 连接池已关闭")

class MongoDBManager:
    """MongoDB操作管理器"""

    def __init__(self):
        self.connection_pool = MongoDBConnectionPool()

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            if self.connection_pool.is_connected():
                # 检查目标数据库
                db = self.connection_pool.get_database()
                collection_names = db.list_collection_names()
                logger.info(f"📁 数据库 '{config.mongodb.database}' 中的集合: {collection_names}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ 连接测试失败: {e}")
            return False

    def insert_one(self, data: Dict[str, Any], collection_name: str, database_name: str = None) -> bool:
        """插入单条数据到指定集合"""
        try:
            collection = self.connection_pool.get_collection(collection_name, database_name)
            result = collection.insert_one(data)
            logger.debug(f"✅ 插入数据到 {collection_name} 成功, ID: {result.inserted_id}")
            return True
        except PyMongoError as e:
            logger.error(f"❌ 插入数据到 {collection_name} 失败: {e}")
            return False

    def insert_many(self, data_list: List[Dict[str, Any]], collection_name: str, database_name: str = None) -> bool:
        """批量插入数据到指定集合"""
        try:
            if not data_list:
                return True

            collection = self.connection_pool.get_collection(collection_name, database_name)
            result = collection.insert_many(data_list, ordered=False)
            logger.info(f"✅ 批量插入数据到 {collection_name} 成功, 数量: {len(result.inserted_ids)}")
            return True
        except PyMongoError as e:
            logger.error(f"❌ 批量插入数据到 {collection_name} 失败: {e}")
            return False

    def find(self, collection_name: str, query: Dict[str, Any] = None,
             limit: int = 0, database_name: str = None) -> List[Dict[str, Any]]:
        """从指定集合查询数据"""
        try:
            if query is None:
                query = {}

            collection = self.connection_pool.get_collection(collection_name, database_name)
            cursor = collection.find(query).limit(limit)
            return list(cursor)
        except PyMongoError as e:
            logger.error(f"❌ 从 {collection_name} 查询数据失败: {e}")
            return []

    def count(self, collection_name: str, query: Dict[str, Any] = None,
             database_name: str = None) -> int:
        """统计指定集合的数据数量"""
        try:
            if query is None:
                query = {}

            collection = self.connection_pool.get_collection(collection_name, database_name)
            return collection.count_documents(query)
        except PyMongoError as e:
            logger.error(f"❌ 统计 {collection_name} 数据失败: {e}")
            return 0

    def close(self):
        """关闭连接池"""
        self.connection_pool.close()

# 全局数据库管理器实例
db_manager = MongoDBManager()
