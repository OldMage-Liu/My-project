#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
持久化上下文 + 完全模拟浏览器 + 退出前等待落盘
（修复选择器拼接问题，正确合并多类评论结构）
"""
import datetime
from math import nan
import time
import pandas as pd
import os as os_alias
import time as time_alias
from playwright.sync_api import sync_playwright as playwright_alias
import random as rnd
import unicodedata

# 1. 持久化用户数据目录
USER_PROFILE_PATH = os_alias.path.join(os_alias.getcwd(), "chrome_profile")

# 2. 启动参数
CHROME_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-web-security",
    "--disable-site-isolation-trials",
    "--disable-features=IsolateOrigins,site-per-process",
    "--disable-infobars",
    "--window-size=1366,768",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding"
]

def _norm_text(s: str) -> str:
    if s is None:
        return ""
    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    zeros = ["\u200b", "\u200c", "\u200d", "\ufeff"]
    for z in zeros:
        s = s.replace(z, "")
    return s.strip()

def _center_y(box):
    if not box:
        return None
    return box["y"] + box["height"] / 2.0

def _nearest_index_by_y(src_box, candidate_boxes, used_idx):
    y0 = _center_y(src_box)
    if y0 is None:
        return None
    best_i, best_d = None, None
    for i, b in enumerate(candidate_boxes):
        if i in used_idx or not b:
            continue
        y1 = _center_y(b)
        if y1 is None:
            continue
        d = abs(y0 - y1)
        if best_d is None or d < best_d:
            best_d, best_i = d, i
    return best_i

def is_xpath(selector: str) -> bool:
    return selector.startswith(('//', '/html', 'xpath='))

def get_all_elements_sync(page, sel1, sel2):
    """合并两个选择器（支持 XPath 和 CSS）的结果为 ElementHandle 列表"""
    loc1 = page.locator(f'xpath={sel1}' if is_xpath(sel1) else sel1)
    loc2 = page.locator(f'xpath={sel2}' if is_xpath(sel2) else sel2)
    handles1 = loc1.element_handles()
    handles2 = loc2.element_handles()
    return handles1 + handles2

def run_main():
    current_crawl_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_counter = 0
    with playwright_alias() as pw:

        browser_context = pw.chromium.launch_persistent_context(
            user_data_dir=USER_PROFILE_PATH,
            channel="chrome",
            headless=False,
            args=CHROME_LAUNCH_ARGS,
            viewport={"width": 1366, "height": 768},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            ignore_default_args=["--enable-automation"]
        )

        main_page = browser_context.new_page()
        main_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = window.chrome || {};
        """)

        # 进入抖音首页并搜索
        main_page.goto("https://www.douyin.com/", timeout=5000)
        main_page.wait_for_timeout(800)
        search_input = main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/div[1]/input')
        search_input.fill('2000元手机推荐2025')
        main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/button/span').click()

        # 点击第一个视频
        main_page.locator('#search-content-area > div > div.LI8kV7Vf > div.oo_LwYT3.NpAMJIe0.p576IkAN > div.NqYoxcpn > div > div > span:nth-child(3)').click()
        main_page.wait_for_timeout(1000)
        right_panel = main_page.locator('//*[@id="douyin-right-container"]/div[3]')
        right_panel.wait_for(state='visible')

        # 滚动加载视频列表
        for _ in range(20):
            for __ in range(5):
                right_panel.evaluate('el => el.scrollTop += el.clientHeight / 5')
                main_page.wait_for_timeout(rnd.randint(20, 50))
            main_page.wait_for_timeout(200)
        main_page.wait_for_timeout(800)

        video_cards  = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div').all()
        video_titles = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[1]').all()
        author_infos = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[2]').all()
        # print("共抓到", len(video_cards), "个卡片")

        # 逐视频处理
        for card_item, title_item, author_item in zip(video_cards, video_titles, author_infos):

            author_text = author_item.text_content()
            file_counter += 1
            video_title_text = title_item.text_content()

            card_item.scroll_into_view_if_needed()
            card_item.click()
            main_page.wait_for_timeout(1000)
            main_page.keyboard.press("x")  # 关闭弹幕

            comment_container = main_page.locator('#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm')
            comment_container.wait_for(state='visible')

            # 滚动加载评论
            for _ in range(50):
                for __ in range(5):
                    comment_container.evaluate('el => el.scrollTop += el.clientHeight / 5')
                    main_page.wait_for_timeout(rnd.randint(20, 50))
            # 点击“展开回复”
            buttons = main_page.query_selector_all('#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm > div > div > div.EpsntdUI > button')
            for btn in buttons:
                try:
                    btn.click()
                except:
                    pass
            # ===== 逐步往上翻 =====
            scroll_step = -200          # 每次向上 200 px
            delay       = 300           # ms
            current_top = comment_container.evaluate('el => el.scrollTop')
            while current_top > 0:
                comment_container.evaluate(
                    f'el => el.scrollTop = Math.max(0, el.scrollTop + {scroll_step})'
                )
                main_page.wait_for_timeout(delay)
                current_top = comment_container.evaluate('el => el.scrollTop')
            # ===== 向上翻结束 =====
            time.sleep(5)
            names=main_page.locator('#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm > div > div > div.EpsntdUI > div.CCbmLKh0.replyContainer > div > div.EpsntdUI > div > div.jzhUi9rG.comment-item-info-wrap > div._uYOTNYZ > div,''#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm > div > div > div.EpsntdUI > div.Vrj4Q3zT.fiDvPS80 > div.jzhUi9rG.comment-item-info-wrap > div._uYOTNYZ > div')
            user_spans = names.all()  # 获取所有匹配的元素列表
            usernames = [span.text_content() for span in user_spans]

            avatar_imgs =main_page.locator('#merge-all-comment-container .comment-mainContent .comment-item-avatar img')
            # 在浏览器中执行 JS：对每个元素，把 img 替换为 "[图片]"，然后取纯文本
            comments_text = main_page.eval_on_selector_all(
                '#merge-all-comment-container .C7LroK_h',  # 可简化选择器
                '''
                (elements) => {
                    return elements.map(el => {
                        // 克隆节点避免修改原页面
                        const clone = el.cloneNode(true);

                        // 把所有 <img> 替换成文本 "[图片]"
                        const imgs = clone.querySelectorAll('img');
                        imgs.forEach(img => {
                            const placeholder = document.createTextNode("[图片]");
                            img.replaceWith(placeholder);
                        });

                        // 返回纯文本内容（自动忽略所有标签）
                        return clone.textContent.trim();
                    });
                }
                '''
            )

            user_infos = []  # 每个元素是字符串 或 nan
            count = avatar_imgs.count()
            for i in range(count):
                try:
                    with main_page.context.expect_page() as new_page_info:
                        avatar_imgs.nth(i).click()
                    new_page = new_page_info.value

                    # ⏳ 等待关键元素
                    new_page.wait_for_selector('#user_detail_element h1', timeout=8000)

                    # 提取 span 文本并拼接
                    texts = new_page.eval_on_selector_all(
                        '#user_detail_element p span',
                        'els => els.map(el => el.textContent.trim()).filter(t => t.length > 0)'
                    )

                    if texts:
                        info_str = " | ".join(texts)
                        user_infos.append(info_str)
                    else:
                        user_infos.append(nan)

                    new_page.close()

                except Exception as e:

                    user_infos.append(nan)
                    # 确保即使出错也关闭页面（加 finally 更安全）
                    try:
                        new_page.close()
                    except:
                        pass

            min_len = min(len(usernames), len(comments_text), len(user_infos))


            data = []
            for i in range(min_len):
                name = usernames[i]
                comment = comments_text[i]
                info = user_infos[i]
                info_display = info if not pd.isna(info) else "nan"

                data.append({
                    "抓取时间": current_crawl_time,  # 脚本运行时间
                    "视频标题": video_title_text,  # 来自你之前提取的变量
                    "作者与视频发布时间": author_text,
                    "用户名": name,
                    "评论内容": comment,
                    "IP/资料": info_display
                })


            df = pd.DataFrame(data)
            safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in str(video_title_text)[:50])
            filename = f"douyin_{safe_title}.csv".replace("/", "_").replace("\\", "_")
            df.to_csv(filename, index=False, encoding="utf-8-sig")

            print(f"✅ 评论已保存到 {filename}")










            input()
            close_btn=main_page.locator('#douyin-right-container > div:nth-child(4) > div > div:nth-child(1) > div.uRH5Oxnw.isDark > div')
            close_btn.wait_for(state='visible', timeout=5000)
            close_btn.dispatch_event('click')
            right_panel.wait_for(state="visible")
        input("按回车退出...")
        browser_context.close()
        time_alias.sleep(3)
        print("[INFO] 会话已安全落盘")

if __name__ == "__main__":
    run_main()  # ✅ 直接调用，没问题