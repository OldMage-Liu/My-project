#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
持久化上下文 + 完全模拟浏览器 + 退出前等待落盘
（修复选择器拼接问题，正确合并多类评论结构）
"""
from math import nan
import time
import pandas as pd_alias
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
        search_input.fill('显卡推荐')
        main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/button/span').click()

        # 点击第一个视频
        main_page.locator('#search-content-area > div > div.LI8kV7Vf > div.oo_LwYT3.NpAMJIe0.p576IkAN > div.NqYoxcpn > div > div > span:nth-child(3)').click()
        main_page.wait_for_timeout(1000)
        right_panel = main_page.locator('//*[@id="douyin-right-container"]/div[3]')
        right_panel.wait_for(state='visible')

        # 滚动加载视频列表
        for _ in range(10):
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
            for _ in range(5):
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
            # === 修复：使用真实 ElementHandle 列表 ===
            comment_span_sel  = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[2]/span'
            comment_span_sel1 = '#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm > div > div > div.EpsntdUI > div.CCbmLKh0.replyContainer > div > div.EpsntdUI > div > div.C7LroK_h > span'

            username_span_sel = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[1]/div[1]/div/a/span/span/span/span/span/span/span'
            username_span_sel1 = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div[2]/div/div/div/div/div/div/a[1]/span/span/span/span/span/span/span'

            user_link_sel     = '#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm > div > div > div.ElTDPJYl.comment-item-avatar > div > a'
            user_link_sel1    = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div/div[1]/div/a'

            comm_a = get_all_elements_sync(main_page, comment_span_sel, '//dummy')
            comm_b = get_all_elements_sync(main_page, comment_span_sel1, '//dummy')
            comment_elements = comm_a + comm_b

            # 2. 用户名
            user_a = get_all_elements_sync(main_page, username_span_sel, '//dummy')
            user_b = get_all_elements_sync(main_page, username_span_sel1, '//dummy')
            username_elements = user_a + user_b


            link_locator = main_page.locator(user_link_sel1)
            link_elements = link_locator.element_handles()


            comment_texts = []
            for el in comment_elements:
                if not el:
                    comment_texts.append(nan)
                    continue
                txt = (el.evaluate("e => e.innerText") or "").strip()
                # 如果元素里只有空格、换行，也算“没文字”
                comment_texts.append(txt if txt else nan)
            username_texts = [
                (el.evaluate("e => e.innerText") or "").strip()
                for el in username_elements if el
            ]
            link_hrefs = [
                el.get_attribute("href")
                for el in link_elements
                if el and el.get_attribute("href")
            ]
            print(len(comment_texts), len(username_texts), len(link_elements))
            ips=[]
            names=[]
            for el in link_elements:
                try:
                    with main_page.context.expect_page() as new_pm:  # 1. 预捕新标签页
                        el.click(timeout=5_000)  # 2. 点击
                    new_page = new_pm.value

                    ip=new_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p')
                    name=new_page.locator('#user_detail_element > div > div.a3i9GVfe.nZryJ1oM._6lTeZcQP.y5Tqsaqg > div.IGPVd8vQ > div.HjcJQS1Z > h1 > span > span')
                    # print(ip.text_content())
                    ips.append(ip.text_content())
                    names.append(name.text_content())
                    new_page.close()  # 4. 立刻关掉
                except Exception:
                    pass
            ip_map = dict(zip(names, ips))

            # 2) name -> comment
            comment_map = dict(zip(username_texts, comment_texts))

            # 3) 按 names 顺序合并
            merged = [(n, ip_map.get(n), comment_map.get(n))  # 找不到为 None
                      for n in names]

            # 4) 打印
            for name, ip, comment in merged:
                print(name, ip, comment)






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
    run_main()