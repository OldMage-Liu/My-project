import cv2
import numpy as np
import mss
from ultralytics import YOLO
import signal

# ========== 配置 ==========
MODEL_PATH = "yolo11x.pt"  # 替换为你的模型路径
OUTPUT_VIDEO = "screen1_to_screen2_output.mp4"

# 全局控制
running = True


def signal_handler(sig, frame):
    global running
    print("\n接收到中断信号，正在退出...")
    running = False


signal.signal(signal.SIGINT, signal_handler)

# ========== 加载模型 ==========
print("正在加载 YOLO 模型...")
model = YOLO(MODEL_PATH)

with mss.mss() as sct:
    monitors = sct.monitors  # [0]=虚拟全屏, [1]=主屏, [2]=副屏...

    if len(monitors) < 2:
        raise RuntimeError("未检测到至少一块屏幕！")

    input_monitor = monitors[2]  # 👈 识别第一块屏幕（主屏）

    if len(monitors) < 3:
        print("⚠️ 未检测到第二块屏幕，将在主屏显示结果。")
        output_monitor = monitors[2]
    else:
        output_monitor = monitors[1]  # 👈 显示到第二块屏幕

    # 获取输入帧尺寸（用于 VideoWriter）
    temp_img = np.array(sct.grab(input_monitor))
    temp_img = cv2.cvtColor(temp_img, cv2.COLOR_BGRA2BGR)
    h, w = temp_img.shape[:2]

    # 初始化视频写入器
    out = cv2.VideoWriter(
        OUTPUT_VIDEO,
        cv2.VideoWriter_fourcc(*'mp4v'),
        20.0,
        (w, h)
    )

    # 创建显示窗口并移到第二屏
    win_name = "YOLO: Screen 1 → Screen 2"
    cv2.namedWindow(win_name, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(win_name, w, h)  # 可选：保持原始比例
    cv2.moveWindow(win_name, output_monitor["left"], output_monitor["top"])

    print(f"✅ 正在识别第一块屏幕 ({w}x{h})")
    print(f"🖥️  结果将显示在第二块屏幕（左上角坐标: {output_monitor['left']}, {output_monitor['top']}）")
    print(f"📹 视频将保存至: {OUTPUT_VIDEO}")
    print("按 'q' 键 或 Ctrl+C 退出程序")

    frame_count = 0
    try:
        while running:
            # 1. 截取第一块屏幕
            screenshot = sct.grab(input_monitor)
            img = np.array(screenshot)
            img = cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)

            # 2. YOLO 推理
            results = model(img, conf=0.5)
            annotated_frame = results[0].plot()

            # 3. 保存视频
            out.write(annotated_frame)

            # 4. 显示到第二块屏幕
            cv2.imshow(win_name, annotated_frame)
            key = cv2.waitKey(1) & 0xFF
            if key == ord('q'):
                break

            frame_count += 1
            if frame_count % 60 == 0:
                print(f"已处理 {frame_count} 帧...")

    except Exception as e:
        print(f"❌ 运行出错: {e}")

    finally:
        # 清理资源
        out.release()
        cv2.destroyAllWindows()
        print(f"\n✅ 程序结束，视频已保存至: {OUTPUT_VIDEO}")


