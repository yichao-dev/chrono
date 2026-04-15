import pandas as pd
import requests
import time
import json
from datetime import datetime

# ==========================================
# 1. 配置区域
# ==========================================

# 【重要】这是你的后端地址（中转站）
API_URL = "http://127.0.0.1:5000/api/sensor_data"

# 文件路径 (保持你的绝对路径)
FILE_INT = r"C:\Users\cww36\Downloads\human_activity_sensor_data_in_home_environment\human_activity_raw_sensor_data\sensor_sample_int.csv"
FILE_FLOAT = r"C:\Users\cww36\Downloads\human_activity_sensor_data_in_home_environment\human_activity_raw_sensor_data\sensor_sample_float.csv"

# 播放速度 (1=实时, 100=极速)
SPEED_RATE = 100 

def send_to_backend(data):
    """
    发送数据到你的后端中转站
    """
    try:
        # 这里的 timeout 设置短一点，防止网络卡顿时模拟器阻塞
        response = requests.post(API_URL, json=data, timeout=2)
        
        if response.status_code != 200:
            print(f"❌ 后端返回错误: {response.status_code}")
            
    except Exception as e:
        # 如果后端没开，只打印一次警告，避免刷屏
        # print(f"⚠️ 连接后端失败: {e}")
        pass

def process_file(file_path, is_int_file):
    """
    读取长表 CSV 并推送
    """
    print(f"🚀 开始处理: {file_path}")
    
    # 分块读取，每次 2000 行，平衡内存和速度
    chunk_iter = pd.read_csv(file_path, chunksize=2000)
    
    prev_timestamp = None
    total_rows = 0

    for chunk in chunk_iter:
        for index, row in chunk.iterrows():
            total_rows += 1
            
            # --- 1. 数据提取 ---
            timestamp_str = str(row['timestamp'])
            sensor_id = str(row['sensor_id'])
            value = row['value']
            
            if pd.isna(value): continue

            # --- 2. 时间控制 ---
            try:
                current_time_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except:
                current_time_obj = None

            if prev_timestamp and current_time_obj:
                diff = (current_time_obj - prev_timestamp).total_seconds()
                if diff > 0 and SPEED_RATE > 0:
                    time.sleep(diff / SPEED_RATE)
            
            if current_time_obj:
                prev_timestamp = current_time_obj

            # --- 3. 构建 Payload ---
            is_digital = False
            if is_int_file and value in [0, 1]:
                is_digital = True
            
            payload = {
                "device_id": f"sensor_{sensor_id}",
                "device_type": "digital_sensor" if is_digital else "analog_sensor",
                "location": "home_zone_" + sensor_id[:2], # 简单模拟位置
                "value": int(value) if is_digital else float(value),
                "timestamp": timestamp_str,
                "status": "triggered" if (is_digital and value == 1) else "normal"
            }

            # --- 4. 发送 ---
            send_to_backend(payload)
            
            # 每 1000 条打印一次进度
            if total_rows % 1000 == 0:
                print(f"   ...已发送 {total_rows} 条数据...")

if __name__ == "__main__":
    print("🟢 模拟器启动...")
    try:
        process_file(FILE_INT, is_int_file=True)
        # process_file(FILE_FLOAT, is_int_file=False) # 需要时可以取消注释
        print("✅ 发送完毕。")
    except KeyboardInterrupt:
        print("\n🛑 停止。")