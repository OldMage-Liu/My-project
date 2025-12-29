#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
持久化上下文 + 完全模拟浏览器 + 退出前等待落盘
"""
import pandas as pd_alias
import logger as log_alias
import os as os_alias
import time as time_alias
from playwright.sync_api import sync_playwright as playwright_alias
import random as random_alias
# 1. 持久化用户数据目录（相对路径即可）
USER_PROFILE_PATH = os_alias.path.join(os_alias.getcwd(), "chrome_profile")
import random as rand_alias
# 2. 启动参数列表
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


def run_main():
    file_counter = 0
    with playwright_alias() as pw:
        # 3. 创建/复用持久化上下文
        browser_context = pw.chromium.launch_persistent_context(
            user_data_dir=USER_PROFILE_PATH,
            channel="chrome",                 # 使用本地安装的官方 Chrome（可选）
            headless=False,                   # 调试阶段设为 False
            args=CHROME_LAUNCH_ARGS,
            viewport={"width": 1366, "height": 768},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            ignore_default_args=["--enable-automation"]  # 去掉自动化标识
        )

        main_page = browser_context.new_page()

        # 4. 简单反检测脚本（可选）
        main_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            window.chrome = window.chrome || {};
        """)

        # 5. 进入目标站点
        main_page.goto("https://www.douyin.com/", timeout=60000)

        search_input = main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/div[1]/input')
        main_page.wait_for_timeout(300)
        search_input.fill('内存条推荐')
        main_page.wait_for_timeout(300)
        main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/button/span').click()
        # 等待节点渲染
        main_page.locator('#search-content-area > div > div.LI8kV7Vf > div.oo_LwYT3.NpAMJIe0.p576IkAN > div.NqYoxcpn > div > div > span:nth-child(3)').click()#点击视频
        main_page.wait_for_timeout(200)
        right_panel = main_page.locator('//*[@id="douyin-right-container"]/div[3]')
        right_panel.wait_for(state='visible')
        # 滚动 5 次
        import random as rnd
        for _ in range(10):#多少个视频
            print(_)
            # 模拟自然滚动
            for _ in range(5):  # 每次循环滚动 5 次
                right_panel.evaluate('el => el.scrollTop += el.clientHeight / 5')  # 每次滚动视口高度的 1/5
                main_page.wait_for_timeout(rnd.randint(20, 50))  # 随机延迟 10-30 毫秒
            current_scroll_pos = right_panel.evaluate("el => el.scrollTop")
            total_scroll_height = right_panel.evaluate("el => el.scrollHeight")
            client_height_val = right_panel.evaluate("el => el.clientHeight")
            main_page.wait_for_timeout(200)
        video_cards = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div').all()
        print("共抓到", len(video_cards), "个卡片")
        video_titles = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[1]').all()
        author_infos = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[2]').all()
        # 3. 依次点击
        for card_item, title_item, author_item in zip(video_cards, video_titles, author_infos):
            author_text = author_item.text_content()
            print(author_text)
            file_counter += 1
            user_data = []
            video_title_text = title_item.text_content()
            # print(title_item.text_content())
            card_item.scroll_into_view_if_needed()
            card_item.click()
            main_page.wait_for_timeout(800)
            main_page.keyboard.press("x")
            comment_container = main_page.locator('#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm')
            comment_container.wait_for(state='visible')
            # 滚动 5 次
            comment_texts = []
            for _ in range(50):#多少条评论
                # 模拟自然滚动
                for _ in range(5):  # 每次循环滚动 5 次
                    comment_container.evaluate('el => el.scrollTop += el.clientHeight / 5')  # 每次滚动视口高度的 1/5
                    main_page.wait_for_timeout(rnd.randint(20, 50))  # 随机延迟 10-30 毫秒
            comment_spans = main_page.locator('//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[2]/span')
            # 获取元素总数
            username_spans = main_page.locator('//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[1]/div[1]/div/a/span/span/span/span/span/span/span')
            comment_contents = comment_spans.all_inner_texts()
            username_contents = username_spans.all_inner_texts()
            comment_list = []
            for username, comment in zip(username_contents, comment_contents):
                if username == '':
                    comment_list.append({
                        '评论': comment,
                        'name': 'NaN',
                        '作者时间': author_text
                    })
                elif comment == '':
                    comment_list.append({
                        '评论': 'NaN',
                        'name': username,
                        '作者时间': author_text
                    })
                else:
                    comment_list.append({
                        '评论': comment,
                        'name': username,
                        '作者时间': author_text
                    })
            comments_df = pd_alias.DataFrame(comment_list)
            user_link_selector = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[1]/div/a'
            user_links = main_page.locator(user_link_selector).all()  # 调用 all() 方法获取所有匹配的元素

            # 遍历所有元素
            for link in user_links:
                link.scroll_into_view_if_needed()
                # 点击元素
                try:
                    link.click()
                    # 等待新页面加载完成
                    popup_page = main_page.wait_for_event("popup")  # 等待新页面出现
                    popup_page.wait_for_load_state()  # 等待新页面加载完成
                    popup_page.wait_for_timeout(rnd.randint(20, 50))
                    # 在新页面中获取所需的数据
                    name_elem = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/div[1]/h1/span/span/span/span/span/span')
                    user_id_elem = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[1]')
                    try:
                        ip_elem = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[2]')
                    except:
                        ip_elem = 'NaN'
                    user_name = name_elem.text_content()
                    user_id = user_id_elem.text_content()
                    user_ip = ip_elem.text_content()
                    popup_page.close()

                    user_data.append({
                        'name': user_name,
                        '用户ID': user_id,
                        'IP': user_ip,
                        '视频简介': title_item.text_content()  # 使用 title_item 变量获取视频简介
                    })
                except:
                    pass
            user_df = pd_alias.DataFrame(user_data)
            print(len(comments_df['name']))
            print(len(user_df['name']))
            merged_df = pd_alias.merge(user_df, comments_df, on='name')
            column_order = ['name', '用户ID', 'IP', '评论', '视频简介', '作者时间']
            merged_df = merged_df[column_order]
            merged_df.to_csv(str(file_counter)+'.csv')
            close_btn = main_page.locator(
                '#douyin-right-container > div:nth-child(4) > div > div:nth-child(1) > div.uRH5Oxnw.isDark > div'
            )
            close_btn.wait_for(state='visible')
            close_btn.dispatch_event('click')
            right_panel.wait_for(state="visible")

        # cards=page.locator('xpath=/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[2]/div[1]/div/div/div/div/div/div/div[1]/div').all()
        # for card in cards:
        #     time.sleep(10)
        #     card.click()

        # # 6. 判断是否已登录
        # if "/login" in page.url:
        #
        #     print("[INFO] 首次登录，请手动完成登录流程，完成后直接关闭浏览器窗口即可。")
        #     page.wait_for_event("close")  # 等待用户关闭窗口
        # else:
        #     print("[INFO] 已检测到登录状态，跳过登录页。")

        # 7. 预留 3 秒让 Chrome 把会话写回磁盘
        input()
        browser_context.close()
        time_alias.sleep(3)
        print("[INFO] 会话已安全落盘，可重新运行脚本验证持久化效果。")

if __name__ == "__main__":
    run_main()