import cv2
import numpy as np
from ultralytics import YOLO
from mss import mss
import win32api, win32con

# =======================
#      模型加载
# =======================
model = YOLO("yolo11s.pt")
model.to("cuda")

# =======================
#      屏幕选择
# =======================
sct = mss()
monitor = sct.monitors[2]
offset_x = monitor["left"]
offset_y = monitor["top"]

print("监测屏幕:", monitor)
print("偏移量:", offset_x, offset_y)

# =======================
#     状态变量
# =======================
smooth_center = None
alpha = 0.25
target_index = 0
HEAD_OFFSET_RATIO = 0.75

# 👇 新增：鼠标左键状态记录（用于检测“按下”事件）
left_button_pressed_last = False

def ema_smooth(old, new, a=0.25):
    if old is None:
        return new
    return (1 - a) * old + a * new

# =======================
#       主循环
# =======================
while True:
    sct_img = sct.grab(monitor)
    frame = np.array(sct_img)
    frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2BGR)

    H, W = frame.shape[:2]
    screen_center = np.array([W / 2, H / 2])

    persons = []
    results = model(frame, verbose=False)

    for r in results:
        if r.boxes is None:
            continue
        for b in r.boxes:
            cls = int(b.cls[0])
            if cls != 0:
                continue
            x1, y1, x2, y2 = b.xyxy[0].cpu().numpy()
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2
            head_y = cy - HEAD_OFFSET_RATIO * (cy - y1)
            persons.append(np.array([cx, head_y]))

            # 可视化
            cv2.circle(frame, (int(cx), int(cy)), 4, (0, 255, 0), -1)
            cv2.circle(frame, (int(cx), int(head_y)), 6, (0, 255, 255), -1)

    # ---- 处理目标选择与鼠标控制 ----
    if len(persons) > 0:
        persons_array = np.array(persons)
        distances = np.linalg.norm(persons_array - screen_center, axis=1)
        sorted_indices = np.argsort(distances)
        target_index = min(target_index, len(sorted_indices) - 1)
        chosen_idx = sorted_indices[target_index]
        target = persons_array[chosen_idx]

        smooth_center = ema_smooth(smooth_center, target, alpha)
        cx, cy = smooth_center.astype(int)

        cv2.circle(frame, (cx, cy), 12, (255, 0, 0), 2)
        cv2.putText(
            frame,
            f"Target: #{target_index + 1}/{len(persons)}",
            (10, 30),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (255, 255, 0),
            2
        )

        abs_x = int(cx + offset_x)
        abs_y = int(cy + offset_y)
        win32api.SetCursorPos((abs_x, abs_y))

    else:
        smooth_center = None
        target_index = 0

    # ==============================
    #   鼠标左键单击检测（切换目标）
    # ==============================
    left_button_state = win32api.GetAsyncKeyState(win32con.VK_LBUTTON)
    left_button_pressed_now = bool(left_button_state & 0x8000)  # 最高位为1表示当前按下

    if not left_button_pressed_last and left_button_pressed_now:
        # 检测到“按下”上升沿（单次点击）
        if len(persons) > 0:
            target_index = (target_index + 1) % len(persons)
            print(f"切换目标 → #{target_index + 1}")

    left_button_pressed_last = left_button_pressed_now  # 更新状态

    # ---- 显示画面 ----
    cv2.imshow("YOLO Head Tracker - Click to switch target", frame)

    # ESC 退出
    if cv2.waitKey(1) & 0xFF == 27:
        break

cv2.destroyAllWindows()