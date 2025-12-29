import cv2
import numpy as np
import mss

# 初始化屏幕捕获（全屏）
with mss.mss() as sct:
    monitor = sct.monitors[0]  # 0 表示主显示器全屏；也可指定区域，如 {"top": 100, "left": 100, "width": 800, "height": 600}

    while True:
        # 截图（返回为字节数据）
        screenshot = sct.grab(monitor)
        # 转为 numpy 数组（BGR 格式供 OpenCV 使用）
        img = np.array(screenshot)
        img = cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)  # 去掉 Alpha 通道

        # 👉 在这里插入你的识别逻辑（OCR、模板匹配、目标检测等）

        # 显示画面
        cv2.imshow("Screen Capture", img)

        # 按 'q' 退出
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

cv2.destroyAllWindows()