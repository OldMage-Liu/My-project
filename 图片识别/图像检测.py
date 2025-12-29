import cv2
from ultralytics import YOLO

# ========== 模型部分 ==========
model = YOLO("yolo11x.pt")

def predict_and_detect(chosen_model, img, conf=0.5):
    results = chosen_model(img, conf=conf)
    for r in results:
        for b in r.boxes:
            x1, y1, x2, y2 = map(int, b.xyxy[0])
            label = r.names[int(b.cls)]
            cv2.rectangle(img, (x1, y1), (x2, y2), (255, 0, 0), 2)
            cv2.putText(img, label, (x1, y1 - 8),
                        cv2.FONT_HERSHEY_PLAIN, 1.2, (255, 0, 0), 2)
    return img

# ========== 读写部分 ==========
video_path = r"xxx.mp4"
cap = cv2.VideoCapture(video_path)
if not cap.isOpened():
    raise FileNotFoundError("视频打不开，请检查路径")

w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
fps = int(cap.get(cv2.CAP_PROP_FPS))

out = cv2.VideoWriter("xxxx.mp4",
                      cv2.VideoWriter_fourcc(*'mp4v'),
                      fps, (w, h))

# ========== 显示窗口 ==========
cv2.namedWindow("YOLO-Detect", cv2.WINDOW_NORMAL)   # 可缩放
cv2.resizeWindow("YOLO-Detect", 1280, 720)          # 初始大小
cv2.moveWindow("YOLO-Detect", 100, 100)             # 初始位置

while True:
    ret, frame = cap.read()
    if not ret:
        break

    frame = predict_and_detect(model, frame, conf=0.5)
    out.write(frame)

    cv2.imshow("YOLO-Detect", frame)
    key = cv2.waitKey(1) & 0xFF
    if key == ord('q') or key == 27:   # 27=ESC
        break

# ========== 善后 ==========
cap.release()
out.release()
cv2.destroyAllWindows()