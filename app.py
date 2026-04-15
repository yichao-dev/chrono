from flask import Flask, request, jsonify
from flask_cors import CORS
import requests as req_lib # 用于转发请求
from collections import deque
import threading

app = Flask(__name__)
CORS(app) # 允许跨域，方便以后写前端

# ==========================================
# ⚙️ 配置：3.2 模块的地址
# ==========================================
# 告诉你的后端，3.2 模块在哪里
TARGET_32_URL = "http://127.0.0.1:5001/api/analyze"

# 本地数据存储 (内存缓存)
data_store = deque(maxlen=2000)
data_lock = threading.Lock()

@app.route('/api/sensor_data', methods=['POST'])
def receive_sensor_data():
    """
    1. 接收模拟器数据
    2. 存储到本地
    3. 转发给 3.2 模块
    """
    try:
        json_data = request.get_json()
        if not json_data:
            return jsonify({"error": "No data"}), 400

        # --- A. 本地存储 (供前端大屏使用) ---
        with data_lock:
            data_store.append(json_data)

        # --- B. 转发给 3.2 模块 (关键逻辑) ---
        try:
            # 使用 timeout=1 防止 3.2 处理太慢卡死这里
            # 这里使用同步转发，如果需要极高并发，可以用 threading 开线程转发
            req_lib.post(TARGET_32_URL, json=json_data, timeout=2)
            print(f"🔄 已转发: {json_data['device_id']}")
        except Exception as e:
            # 3.2 模块挂了或者没开，不要影响主程序
            print(f"❌ 转发失败 (可能 3.2 没开): {e}")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/latest_data', methods=['GET'])
def get_data():
    """供前端大屏获取最新数据"""
    with data_lock:
        return jsonify(list(data_store)), 200

@app.route('/')
def index():
    return f"<h1>✅ 中转站后端运行中</h1><p>当前缓存: {len(data_store)} 条</p>"

if __name__ == '__main__':
    print("🟢 中转站启动: http://127.0.0.1:5000")
    app.run(port=5000, threaded=True)