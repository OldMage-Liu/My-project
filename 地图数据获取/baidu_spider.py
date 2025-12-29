"""
百度地图商户数据爬取脚本
功能：自动化爬取广东省各地区的商业机构信息（名称、地址等）
特点：
    - 支持断点续传，可从中断位置继续
    - 使用 Playwright 模拟真实浏览器操作
    - 持久化浏览器配置，避免频繁登录
    - 自动翻页获取多页数据
"""

from 广东省三级城镇获取 import 广东地区

from config.settings import config
import time
import os
import unicodedata
from playwright.sync_api import sync_playwright, TimeoutError
import json
import logging
from datetime import datetime
from database.mongodb import db_manager
import gc  # 垃圾回收模块
import psutil  # 进程监控模块

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 获取当前进程对象（用于内存监控）
current_process = psutil.Process()


def get_memory_info():
    """
    获取当前进程的内存使用信息
    
    Returns:
        str: 格式化的内存使用信息
    """
    mem_info = current_process.memory_info()
    mem_mb = mem_info.rss / 1024 / 1024  # 转换为MB
    return f"{mem_mb:.2f} MB"


def clean_memory():
    """
    主动清理内存和触发垃圾回收
    """
    gc.collect()  # 强制垃圾回收
    logger.info(f"🧹 内存清理完成 - 当前使用: {get_memory_info()}")


# ==================== 全局配置 ====================

# Chrome 用户配置文件路径（用于保存登录状态、Cookie等）
USER_PROFILE_PATH = os.path.join(os.getcwd(), "chrome_profile")

# 进度保存文件（用于断点续传）
STATE_FILE = "progress.json"

# 批量插入数据库的批次大小
BATCH_SIZE = 50

# Chrome 启动参数（用于规避反爬虫检测）
CHROME_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",  # 禁用自动化控制特征
    "--disable-web-security",                          # 禁用Web安全策略
    "--disable-site-isolation-trials",                 # 禁用站点隔离
    "--disable-features=IsolateOrigins,site-per-process",  # 禁用进程隔离
    "--disable-infobars",                              # 禁用信息栏
    "--window-size=1366,768",                          # 设置窗口大小
    "--no-sandbox",                                    # 禁用沙箱模式
    "--disable-dev-shm-usage",                         # 禁用/dev/shm使用
    "--disable-gpu",                                   # 禁用GPU加速
    "--disable-background-timer-throttling",           # 禁用后台计时器节流
    "--disable-backgrounding-occluded-windows",        # 禁用后台窗口
    "--disable-renderer-backgrounding"                 # 禁用渲染器后台化
]


# ==================== 工具函数 ====================

def normalize_text(text: str) -> str:
    """
    标准化文本，移除零宽字符
    
    Args:
        text: 需要标准化的文本字符串
        
    Returns:
        str: 处理后的文本（移除零宽字符并去除首尾空格）
        
    Note:
        零宽字符包括：零宽空格、零宽不连字、零宽连字、零宽非断空格等
    """
    if text is None:
        return ""
    
    try:
        # NFKC标准化：兼容性分解，再进行兼容性组合
        text = unicodedata.normalize("NFKC", text)
    except Exception:
        pass
    
    # 定义常见的零宽字符
    zero_width_chars = [
        "\u200b",  # 零宽空格
        "\u200c",  # 零宽不连字
        "\u200d",  # 零宽连字
        "\ufeff"   # 零宽非断空格（BOM）
    ]
    
    # 移除所有零宽字符
    for char in zero_width_chars:
        text = text.replace(char, "")
    
    return text.strip()


def get_center_y(bounding_box):
    """
    获取元素边界框的Y轴中心坐标
    
    Args:
        bounding_box: 包含元素位置信息的字典，格式为 {'x': ..., 'y': ..., 'width': ..., 'height': ...}
        
    Returns:
        float: Y轴中心坐标，如果边界框无效则返回 None
    """
    if not bounding_box:
        return None
    return bounding_box["y"] + bounding_box["height"] / 2.0


def find_nearest_index_by_y(source_box, candidate_boxes, used_indices):
    """
    在候选边界框列表中查找与源边界框Y轴距离最近的未使用索引
    
    Args:
        source_box: 源边界框（参照物）
        candidate_boxes: 候选边界框列表
        used_indices: 已使用的索引集合
        
    Returns:
        int or None: 最近的未使用索引，如果没有有效候选则返回 None
        
    Note:
        此函数可用于智能匹配页面元素，例如将名称和地址按Y轴位置配对
    """
    source_y = get_center_y(source_box)
    if source_y is None:
        return None
    
    best_index, best_distance = None, None
    
    # 遍历所有候选框，找到Y轴距离最近的
    for index, box in enumerate(candidate_boxes):
        # 跳过已使用的索引
        if index in used_indices:
            continue
        
        candidate_y = get_center_y(box)
        if candidate_y is None:
            continue
        
        # 计算Y轴距离
        distance = abs(source_y - candidate_y)
        
        # 更新最优解
        if best_distance is None or distance < best_distance:
            best_distance, best_index = distance, index
    
    return best_index


def batch_save_to_mongodb(data_batch, keyword, area):
    """
    批量保存数据到MongoDB
    
    Args:
        data_batch: 数据列表，每个元素包含 name, address, phone
        keyword: 搜索关键词
        area: 搜索地区
    
    Returns:
        bool: 保存是否成功
    """
    if not data_batch:
        return True
    
    current_time = datetime.now()
    documents = []
    
    for item in data_batch:
        document = {
            "name": item["name"],
            "address": item["address"],
            "phone": item["phone"],
            "keyword": keyword,
            "area": area,
            "type": "线下门店",
            "source": "百度地图",
            "created_at": current_time,
            "updated_at": current_time
        }
        documents.append(document)
    
    try:
        # 使用批量插入
        success = db_manager.insert_many(
            data_list=documents,
            collection_name=config.mongodb.collection
        )
        if success:
            logger.info(f"✅ 批量保存 {len(documents)} 条 - {keyword} | {area}")
            return True
        else:
            logger.error(f"❌ 批量保存失败 - {keyword} | {area}")
            return False
    except Exception as e:
        logger.error(f"❌ 批量保存异常: {e}")
        return False


# ==================== 主程序 ====================

def run_main():
    """
    主函数：执行百度地图数据爬取任务
    
    工作流程：
        1. 加载上次的进度（如果存在）
        2. 启动 Playwright 浏览器
        3. 遍历所有关键词和地区组合
        4. 对每个组合进行搜索并翻页获取数据
        5. 保存数据（可选：存入数据库）
        6. 完成后清理断点文件
    """
    
    # 记录初始内存使用情况
    logger.info(f"🚀 程序启动 - 初始内存: {get_memory_info()}")
    
    # ========== 第一步：加载进度 ==========
    progress = {"area_index": 0, "keyword_index": 0}
    
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
            logger.info(f"📥 从断点恢复: 地区索引={progress['area_index']}, 关键词索引={progress['keyword_index']}")
        except Exception:
            logger.warning("⚠️ 读取断点文件失败，从头开始")

    # ========== 第二步：启动浏览器 ==========
    with sync_playwright() as playwright:
        
        # 使用持久化上下文（保存Cookie、登录状态等）
        browser_context = playwright.chromium.launch_persistent_context(
            user_data_dir=USER_PROFILE_PATH,      # 用户数据目录
            channel="chrome",                      # 使用本地Chrome浏览器
            headless=False,                        # 非无头模式（显示浏览器窗口）
            args=CHROME_LAUNCH_ARGS,              # 启动参数
            viewport={"width": 1366, "height": 768},  # 视口大小
            locale="zh-CN",                        # 语言设置
            timezone_id="Asia/Shanghai",           # 时区设置
            ignore_default_args=["--enable-automation"]  # 忽略自动化标识
        )

        # 创建新页面
        main_page = browser_context.new_page()

        # 注入JavaScript以隐藏webdriver特征（反爬虫）
        main_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = window.chrome || {};
        """)

        # ========== 第三步：打开百度地图 ==========
        main_page.goto("https://map.baidu.com/", timeout=5000)
        time.sleep(1)  # 等待页面加载

        # ========== 第四步：定义搜索关键词 ==========
        keywords = [
            # 酒店/住宿 10
            '五星级酒店', '精品酒店', '度假酒店', '商务酒店', '国际酒店', '高端民宿', '客栈', '温泉酒店', '设计师酒店',
            '公寓式酒店',

            # 餐饮/酒吧 15
            '粤菜馆', '茶餐厅', '江浙菜', '私房菜', '融合菜', '日料', '韩料', '法餐', '意大利餐厅', '雪茄吧',
            '威士忌吧', '精酿啤酒吧', '红酒庄', '高端烧烤', '高端火锅',

            # 咖啡/茶饮 5
            '咖啡厅', '独立咖啡馆', '高端茶饮', '功夫茶', '茶庄',

            # 休闲娱乐 15
            '商务KTV', '高端棋牌室', '私人会所', '俱乐部', '水疗', 'SPA', '温泉', '足浴', '按摩', '高端影院', '剧本杀',
            '密室逃脱', '飞行俱乐部', '潜水俱乐部', '游艇俱乐部',

            # 美业/医养 15
            '医疗美容', '口腔医院', '眼科医院', '植发', '抗衰中心', '体检中心', '月子中心', '皮肤管理', '高端美甲',
            '高端美睫', '半永久', '高端健身房', '瑜伽馆', '中医馆', '高端宠物医院',

            # 零售/奢侈品 15
            '奢侈品', '名表', '珠宝', '钻石', '翡翠', '黄金', '买手店', '设计师品牌', '高端彩妆', '高端童装',
            '进口母婴', '高端家电', '高端音响', '豪车展厅', '摩托车俱乐部',

            # 文化/艺术 10
            '书店', '书吧', '艺术画廊', '文化馆', '博物馆', '剧院', '音乐厅', '国学馆', '书法馆', '美术馆',

            # 体育/户外 10
            '高尔夫练习场', '马术俱乐部', '击剑馆', '室内滑雪场', '室内攀岩', '射击俱乐部', '网球俱乐部',
            '羽毛球俱乐部', '潜水中心', '滑雪俱乐部',

            # 教培/亲子 10
            '国际学校', '高端幼儿园', '留学机构', '少儿英语', '少儿编程', '少儿美术', '少儿舞蹈', '少儿钢琴',
            '亲子游泳馆', '早教中心',

            # 空间/服务 10
            '办公楼', '产业园', '联合办公', '婚礼策划', '活动场地', '高端摄影', '高端打印', '奢侈品养护', '高端搬家',
            '高端保洁',

            # 烟酒/特产 5
            '烟酒', '雪茄', '高端白酒', '红酒庄', '特产店',

            # 汽车/出行 5
            '豪车维修', '高端洗车', '高端贴膜', '超跑俱乐部', '高端租车',

            # 其他高客单 5
            '高端家政', '高端管家', '高端旅行定制', '私人银行', '高端保险'
        ]

        # 获取广东省所有地区列表
        areas = list(广东地区())

        # ========== 第五步：双重循环遍历关键词和地区 ==========
        for keyword_index, keyword in enumerate(keywords):
            # 跳过已完成的关键词
            if keyword_index < progress.get("keyword_index", 0):
                continue
            
            for area_index, area in enumerate(areas):
                # 跳过已完成的地区（仅针对当前关键词）
                if keyword_index == progress.get("keyword_index", 0) and area_index < progress.get("area_index", 0):
                    continue
                
                # ========== 保存当前进度 ==========
                with open(STATE_FILE, "w", encoding="utf-8") as f:
                    json.dump({"area_index": area_index, "keyword_index": keyword_index}, f)

                # ========== 执行搜索 ==========
                search_query = area + keyword  # 组合搜索词，例如："广州市越秀区酒店"
                logger.info(f"🔍 正在搜索: {search_query}")
                
                # 定位搜索框并输入搜索词
                search_input = main_page.locator('xpath=/html/body/div[1]/div[2]/div/div[1]/div/input')
                search_input.wait_for(state='visible', timeout=5000)
                search_input.fill(search_query)
                
                # 点击搜索按钮
                main_page.locator('#search-button').click()
                time.sleep(1.5)  # 等待搜索结果加载

                # ========== 翻页获取数据 ==========
                previous_page_names = []  # 用于检测是否到达最后一页
                data_batch = []  # 批量数据缓存
                
                # 最多翻30页
                for page_num in range(30):
                    try:
                        # 等待最多 5 秒，直到“加载更多”按钮出现并可点
                        more = main_page.locator('[id^="card-"] div.poi-wrapper ul li.more-result a')
                        more.wait_for(state='visible', timeout=5000)
                        more.click()
                    except :
                        # 没出现就跳过，继续找下一页
                        pass
                    try:
                        # 查找"下一页"按钮
                        next_button = main_page.query_selector('xpath=//p/span/a[@tid="toNextPage"]')
                        if not next_button:
                            break

                        # 检查"下一页"按钮是否被禁用
                        is_disabled = (
                            "disabled" in (next_button.get_attribute("class") or "") or
                            next_button.is_hidden() or
                            not next_button.is_visible()
                        )

                        # ========== 获取当前页的数据 ==========
                        
                        # 定位名称元素
                        name_locator = main_page.locator(
                            'xpath=//div/div[1]/ul/li/div[1]/div[3]/div[1]/span[1]/a[@class="n-blue"]'
                        )
                        current_page_names = name_locator.all_text_contents()
                        
                        # 检测是否重复（说明已经到达最后一页）
                        if current_page_names and previous_page_names == current_page_names:
                            break
                        else:
                            previous_page_names = current_page_names

                        # 定位地址元素
                        address_locator = main_page.locator(
                            'xpath=//div/div[1]/ul/li/div[1]/div/div/span[@class="n-grey"]'
                        )
                        current_page_addresses = address_locator.all_text_contents()

                        numbers=[]
                        for ii in range(1,10):
                            phone_numbers=main_page.locator(
                                'xpath=//div/div/ul/li['+str(ii)+']/div/div/div[@class="row tel"]')
                            numbers.append(phone_numbers.all_text_contents())
                        
                        # ========== 处理数据（添加到批次缓存） ==========
                        for name, address, number in zip(current_page_names, current_page_addresses, numbers):
                            # 清理数据
                            clean_name = normalize_text(name.strip())
                            clean_address = normalize_text(address.strip())
                            clean_number = normalize_text(str(number[0]) if number and len(number) > 0 else "")
                            
                            # 添加到批次缓存
                            data_item = {
                                "name": clean_name,
                                "address": clean_address,
                                "phone": clean_number
                            }
                            data_batch.append(data_item)
                            
                            # 当批次达到指定大小时，批量保存
                            if len(data_batch) >= BATCH_SIZE:
                                batch_save_to_mongodb(data_batch, keyword, area)
                                data_batch = []  # 清空批次

                        # ========== 点击下一页 ==========
                        next_button.click()
                        main_page.wait_for_timeout(1000)  # 等待页面加载
                        
                    except Exception as error:
                        logger.error(f"❌ 翻页异常: {error}")
                        break
                
                # ========== 保存剩余的数据批次 ==========
                if data_batch:
                    batch_save_to_mongodb(data_batch, keyword, area)
                    data_batch = []  # 清空批次
                
                # 释放临时变量内存
                previous_page_names = []
                
                # 每10个地区清理一次内存
                if area_index % 10 == 0:
                    clean_memory()

        # ========== 第六步：清理断点文件 ==========
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            logger.info("🎉 所有任务已完成")

        # ========== 第七步：清理并关闭浏览器 ==========
        # 最终内存清理
        clean_memory()
        
        browser_context.close()
        time.sleep(3)
        
        logger.info(f"✅ 程序结束 - 最终内存: {get_memory_info()}")

# ==================== 程序入口 ====================
if __name__ == "__main__":
    run_main()
