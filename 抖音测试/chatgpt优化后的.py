#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
持久化上下文 + 完全模拟浏览器 + 退出前等待落盘
（保留原始 XPath 选择器，仅增加 profile_url 列并用其合并用户与评论数据）
"""

import time
import pandas as pd_alias
import logger as log_alias
import os as os_alias
import time as time_alias
from playwright.sync_api import sync_playwright as playwright_alias
import random as random_alias
import unicodedata
import random as rand_alias

# 1. 持久化用户数据目录（相对路径即可）
USER_PROFILE_PATH = os_alias.path.join(os_alias.getcwd(), "chrome_profile")

# 2. 启动参数列表（优化浏览器性能和反检测）
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

# 文本标准化函数，清理不可见字符
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

# 计算元素垂直中心坐标
def _center_y(box):
    if not box:
        return None
    return box["y"] + box["height"] / 2.0

# 在候选框中找到垂直距离最近且未使用的索引
def _nearest_index_by_y(src_box, candidate_boxes, used_idx):
    y0 = _center_y(src_box)
    if y0 is None:
        return None
    best_i, best_d = None, None
    for i, b in enumerate(candidate_boxes):
        if i in used_idx:
            continue
        y1 = _center_y(b)
        if y1 is None:
            continue
        d = abs(y0 - y1)
        if best_d is None or d < best_d:
            best_d, best_i = d, i
    return best_i

def run_main():
    file_counter = 0
    with playwright_alias() as pw:

        # 3. 创建/复用持久化上下文
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
        # 4. 简单反检测脚本（可选）
        main_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = window.chrome || {};
        """)

        # 5. 进入目标站点并搜索视频
        main_page.goto("https://www.douyin.com/", timeout=5000)
        main_page.wait_for_timeout(800)
        search_input = main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/div[1]/input')
        search_input.fill('恋与深空')
        main_page.locator('//*[@id="douyin-header"]/div[1]/header/div/div/div[1]/div/div[2]/div/button/span').click()

        # 点击第一个视频
        main_page.locator('#search-content-area > div > div.LI8kV7Vf > div.oo_LwYT3.NpAMJIe0.p576IkAN > div.NqYoxcpn > div > div > span:nth-child(3)').click()
        main_page.wait_for_timeout(200)
        right_panel = main_page.locator('//*[@id="douyin-right-container"]/div[3]')
        right_panel.wait_for(state='visible')

        # 滚动加载评论列表
        import random as rnd
        for _ in range(10):
            for __ in range(5):
                right_panel.evaluate('el => el.scrollTop += el.clientHeight / 5')
                main_page.wait_for_timeout(rnd.randint(20, 50))
            right_panel.evaluate("el => el.scrollTop")
            right_panel.evaluate("el => el.scrollHeight")
            right_panel.evaluate("el => el.clientHeight")
            main_page.wait_for_timeout(200)
        main_page.wait_for_timeout(800)

        # 获取视频卡片、标题和作者信息
        video_cards  = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div').all()
        video_titles = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[1]').all()
        author_infos = right_panel.locator('//*[@id="search-result-container"]/div[2]/ul/li/div/a/div/div[2]/div/div[2]').all()
        print("共抓到", len(video_cards), "个卡片")

        # 逐卡片处理
        for card_item, title_item, author_item in zip(video_cards, video_titles, author_infos):

            author_text = author_item.text_content()
            file_counter += 1
            video_title_text = title_item.text_content()

            card_item.scroll_into_view_if_needed()
            card_item.click()
            main_page.wait_for_timeout(1000)
            main_page.keyboard.press("x")

            # 评论容器
            comment_container = main_page.locator('#merge-all-comment-container > div > div.Rwb9ssMc.comment-mainContent.ufktjxUm')
            comment_container.wait_for(state='visible')

            # 滚动加载评论
            for _ in range(30):
                for __ in range(5):
                    comment_container.evaluate('el => el.scrollTop += el.clientHeight / 5')
                    main_page.wait_for_timeout(rnd.randint(20, 50))

            # —— 保留原始评论、用户名和用户链接定位器 —— #
            comment_span_sel  = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[2]/span'
            username_span_sel = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[2]/div/div[1]/div[1]/div/a/span/span/span/span/span/span/span'
            user_link_sel     = '//*[@id="merge-all-comment-container"]/div/div[3]/div/div/div[1]/div/a'
            comment_spans  = main_page.locator(comment_span_sel)
            username_spans = main_page.locator(username_span_sel)
            user_link_nodes = main_page.locator(user_link_sel)

            # 计算 bounding box 用于对齐
            uname_count = username_spans.count()
            com_count   = comment_spans.count()
            link_count  = user_link_nodes.count()

            comment_boxes = [comment_spans.nth(i).bounding_box() for i in range(com_count)]
            link_boxes    = [user_link_nodes.nth(i).bounding_box() for i in range(link_count)]
            used_comment_idx = set()
            used_link_idx    = set()
            rows = []
            for i in range(uname_count):
                u_loc  = username_spans.nth(i)
                u_box  = u_loc.bounding_box()
                name_txt = _norm_text((u_loc.text_content() or "").strip()) or "NaN"

                # 匹配最近的评论节点
                ci = _nearest_index_by_y(u_box, comment_boxes, used_comment_idx)
                if ci is not None:
                    used_comment_idx.add(ci)
                    c_txt = _norm_text((comment_spans.nth(ci).text_content() or "").strip()) or "NaN"
                else:
                    c_txt = "NaN"

                # 匹配最近的头像/链接节点
                li = _nearest_index_by_y(u_box, link_boxes, used_link_idx)
                if li is not None:
                    used_link_idx.add(li)
                    try:
                        purl = user_link_nodes.nth(li).get_attribute("href") or "NaN"
                    except Exception:
                        purl = "NaN"
                else:
                    purl = "NaN"
                rows.append({
                    "profile_url": purl,
                    "评论": c_txt,
                    "name": name_txt,
                    "作者时间": author_text,
                    "视频简介": video_title_text
                })
            comments_df = pd_alias.DataFrame(rows)

            # —— 用户信息采集（保留点击逻辑与选择器）——
            user_data = []
            for idx in range(link_count):
                el = user_link_nodes.nth(idx)
                try:
                    purl = el.get_attribute("href") or "NaN"
                except Exception:
                    purl = "NaN"
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    el.click()
                except Exception:
                    continue
                try:
                    popup_page = main_page.wait_for_event("popup", timeout=3000)
                    popup_page.wait_for_timeout(rnd.randint(40, 80))
                except Exception:
                    # 兜底：尝试当前页读取用户信息
                    try:
                        name_elem    = main_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/div[1]/h1/span/span/span/span/span/span')
                        user_id_elem = main_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[1]')
                        try:
                            ip_elem = main_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[2]')
                            user_ip = ip_elem.text_content() or "NaN"
                        except:
                            user_ip = "NaN"
                        user_name = name_elem.text_content() or "NaN"
                        user_id   = user_id_elem.text_content() or "NaN"
                        user_data.append({
                            'profile_url': purl,
                            'name': _norm_text(user_name),
                            '用户ID': _norm_text(user_id),
                            'IP': _norm_text(user_ip)
                        })
                        continue
                    except Exception:
                        continue
                try:
                    name_elem    = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/div[1]/h1/span/span/span/span/span/span')
                    user_id_elem = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[1]')
                    try:
                        ip_elem = popup_page.locator('//*[@id="user_detail_element"]/div/div[2]/div[2]/p/span[2]')
                        user_ip = ip_elem.text_content() or "NaN"
                    except Exception:
                        user_ip = "NaN"
                    user_name = name_elem.text_content() or "NaN"
                    user_id   = user_id_elem.text_content() or "NaN"
                    user_data.append({
                        'profile_url': purl,
                        'name': _norm_text(user_name),
                        '用户ID': _norm_text(user_id),
                        'IP': _norm_text(user_ip)
                    })
                except Exception:
                    user_data.append({
                        'profile_url': purl,
                        'name': 'NaN',
                        '用户ID': 'NaN',
                        'IP': 'NaN'
                    })
                finally:
                    try:
                        popup_page.close()
                    except Exception:
                        pass
            user_df = pd_alias.DataFrame(user_data)

            # 调试信息
            try:
                print("评论 name 数量:", len(comments_df['name']))
            except Exception:
                print("评论数据计数异常")
            try:
                print("用户详情数量:", len(user_df['name']))
            except Exception:
                print("用户详情计数异常")

            # 合并评论与用户数据
            if 'profile_url' not in comments_df.columns:
                comments_df['profile_url'] = 'NaN'
            if 'profile_url' not in user_df.columns:
                user_df['profile_url'] = 'NaN'
            merged_df = pd_alias.merge(
                comments_df,
                user_df,
                on='profile_url',
                how='left',
                suffixes=('_评论区', '_主页')
            )

            # 优先用主页的 name，否则用评论区的 name
            if 'name_主页' in merged_df.columns:
                merged_df['name'] = merged_df['name_主页'].where(
                    merged_df['name_主页'].notna() & (merged_df['name_主页'] != 'NaN'),
                    merged_df.get('name_评论区', 'NaN')
                )
            elif 'name_评论区' in merged_df.columns:
                merged_df['name'] = merged_df['name_评论区']
            elif 'name' not in merged_df.columns:
                merged_df['name'] = 'NaN'

            # 确保存在的列
            for col in ['用户ID', 'IP', '评论', '视频简介', '作者时间', 'profile_url']:
                if col not in merged_df.columns:
                    merged_df[col] = merged_df.get(col, 'NaN')

            # 列顺序
            column_order = ['name', '用户ID', 'IP', '评论', '视频简介', '作者时间', 'profile_url']
            for col in column_order:
                if col not in merged_df.columns:
                    merged_df[col] = "NaN"
            merged_df = merged_df[column_order]

            # 保存 CSV
            merged_df.to_csv(str(file_counter) + '.csv', index=False, encoding='utf-8-sig')

            # 关闭视频弹层
            try:
                close_btn = main_page.locator(
                    '#douyin-right-container > div:nth-child(4) > div > div:nth-child(1) > div.uRH5Oxnw.isDark > div'
                )
                close_btn.wait_for(state='visible', timeout=5000)
                close_btn.dispatch_event('click')
                right_panel.wait_for(state="visible")
            except Exception:
                pass

        # 7. 预留 3 秒让会话落盘，并等待用户按回车退出
        input()
        browser_context.close()
        time_alias.sleep(3)
        print("[INFO] 会话已安全落盘，可重新运行脚本验证持久化效果。")

if __name__ == "__main__":
    run_main()
