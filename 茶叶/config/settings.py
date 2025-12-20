import os

class MongoDBSettings:
    """MongoDB配置类 - 简化版本"""

    def __init__(self):
        # 直接从连接字符串中提取必要信息
        self.connection_string = "mongodb://root:0t5fF64iPDRgSmq6@dds-wz914cf5b48787242468-pub.mongodb.rds.aliyuncs.com:3717,shengyiadmin.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-96038788"
        self.database = os.getenv("MONGO_DATABASE", "syt_ai_leads")
        # 基础连接配置
        self.collection = "company"
        self.max_pool_size = int(os.getenv("MONGO_MAX_POOL_SIZE", "50"))
        self.socket_timeout_ms = int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "30000"))
        self.connect_timeout_ms = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "20000"))
        self.server_selection_timeout_ms = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "30000"))

class CrawlerSettings:
    """爬虫配置类"""

    def __init__(self):
        self.timeout = int(os.getenv("CRAWLER_TIMEOUT", "30"))
        self.retry_times = int(os.getenv("CRAWLER_RETRY_TIMES", "3"))
        self.delay = float(os.getenv("CRAWLER_DELAY", "1.0"))
        self.user_agent = os.getenv("CRAWLER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self.batch_size = int(os.getenv("CRAWLER_BATCH_SIZE", "100"))
        self.max_buffer_size = int(os.getenv("CRAWLER_MAX_BUFFER_SIZE", "1000"))

class Config:
    """配置管理器"""

    def __init__(self):
        self.mongodb = MongoDBSettings()
        self.crawler = CrawlerSettings()

# 全局配置实例
config = Config()
