#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持久化浏览器上下文 + 只拦截业务接口 Bearer Token
目标域名：baize-api-yunfu.jingxiansuo.com
"""

import os
from playwright.sync_api import sync_playwright

USER_PROFILE_PATH = os.path.join(os.getcwd(), "chrome_profile")

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

def 令牌():
    token = None

    with sync_playwright() as pw:
        browser_context = pw.chromium.launch_persistent_context(
            user_data_dir=USER_PROFILE_PATH,
            channel="chrome",
            headless=True,
            args=CHROME_LAUNCH_ARGS,
            viewport={"width": 1366, "height": 768},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            ignore_default_args=["--enable-automation"]
        )

        page = browser_context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = window.chrome || {};
        """)

        def on_request(request):
            nonlocal token
            url = request.url
            # 只关心业务接口
            if "baize-api-yunfu.jingxiansuo.com" not in url:
                return
            auth = request.headers.get("authorization") or request.headers.get("Authorization")
            if auth and auth.startswith("Bearer "):
                tk = auth[7:].strip()
                token = tk

        page.on("request", on_request)
        url = "https://ai.crmgpt.net/#/clue/clue-c?action=searchCustomer%2Fadvanced-search"
        page.goto(url, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        # 多等几秒，防止异步接口晚到
        page.wait_for_timeout(5000)
        browser_context.close()
    return token

if __name__ == "__main__":
    t = 令牌()
    print("\n✅ 最终捕获 Token：", t)
