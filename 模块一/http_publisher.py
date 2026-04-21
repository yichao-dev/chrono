import pandas as pd
import requests
import time
from datetime import datetime
import threading
import sys
import os

if __name__ == "__main__":
    URL_32 = "http://10.40.156.112:8001/api/v1/3_2/sensor_data" # 模块二算法接口
    URL_34 = "http://10.40.156.146:5000/api/receive_event"     # 模块四Agent 接口
    #文件路径
    FILE_INT = r"C:\Users\cww36\Downloads\human_activity_sensor_data_in_home_environment\human_activity_raw_sensor_data\sensor_sample_int.csv"
    FILE_FLOAT = r"C:\Users\cww36\Downloads\human_activity_sensor_data_in_home_environment\human_activity_raw_sensor_data\sensor_sample_float.csv"
    
    SPEED_RATE = 1 # 模拟速度倍率

    class DataStats:
        def __init__(self):
            self.sent_count_32 = 0
            self.sent_count_34 = 0
            self.lock = threading.Lock()
    
    stats = DataStats()

    # 2.4 发送函数 (Removed global, using stats object)
    def send_to_32(data_payload):
        try:
            payload = {
                "request_type": "sensor_data_input",
                "timestamp": datetime.now().isoformat(),
                "source": "module_3_1",
                "data": [data_payload]
            }
            res = requests.post(URL_32, json=payload, timeout=2)
            if res.status_code == 200:
                with stats.lock:
                    stats.sent_count_32 += 1
                    if stats.sent_count_32 <= 10:
                        print(f"[发送 3.2] Sensor: {data_payload['sensor_id']}, Val: {data_payload['value']} (Count: {stats.sent_count_32})")
        except Exception as e:
            print(f"[Error 3.2] {e}")
            pass

    def send_to_34(text_payload):
        try:
            payload = {
                "msg_type": "sensor_event",
                "content": text_payload,
                "timestamp": datetime.now().isoformat()
            }
            res = requests.post(URL_34, json=payload, timeout=2)
            if res.status_code == 200:
                with stats.lock:
                    stats.sent_count_34 += 1
        except Exception as e:
            print(f"[Error 3.4] {e}")
            pass

    # 处理函数 (Removed global, using stats object)
    def process_file(file_path, is_int):
        try:
            chunk_iter = pd.read_csv(file_path, chunksize=100)
            prev_time = None
            row_count = 0

            for chunk in chunk_iter:
                for _, row in chunk.iterrows():
                    try:
                        # --- 数据清洗 ---
                        ts_str = str(row['timestamp'])
                        sensor_id = str(row['sensor_id'])
                        raw_val = row['value']
                        if pd.isna(raw_val):
                            continue

                        # --- 构建数据 ---
                        if is_int:
                            state = "开启" if int(raw_val) == 1 else "关闭"
                            value_type = "INT"
                            desc_text = f"【传感器事件】{ts_str}，传感器 {sensor_id} 状态变为 {state}。"
                        else:
                            value_type = "FLOAT"
                            desc_text = f"【传感器事件】{ts_str}，传感器 {sensor_id} 读数为 {raw_val:.2f}。"

                        data_32 = {
                            "sensor_id": sensor_id,
                            "node_id": f"NODE_{sensor_id}",
                            "timestamp": ts_str,
                            "value": int(raw_val) if is_int else float(raw_val),
                            "data_type": value_type,
                            "location": f"Room_{sensor_id[0]}",
                            "function": "motion" if is_int else "current"
                        }

                        # --- 发送 ---
                        send_to_32(data_32)
                        send_to_34(desc_text)
                        row_count += 1

                        # --- 时间控制 ---
                        try:
                            curr_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                            if prev_time:
                                delta = (curr_time - prev_time).total_seconds()
                                if 0 < delta < 3600:
                                    time.sleep(delta / SPEED_RATE)
                                elif delta <= 0:
                                    time.sleep(0.005)
                            prev_time = curr_time
                        except:
                            time.sleep(0.005)

                    except Exception as e:
                        print(f"[Row Error] {e}")
                        pass
            print(f"[文件完成] {os.path.basename(file_path)} 处理结束，共处理 {row_count} 行。")
        except Exception as e:
            print(f"[文件读取失败] {file_path}: {e}")

    # 状态打印线程
    def print_status():
        """后台线程：每隔 5 秒打印一次发送状态"""
        while True:
            time.sleep(5)
            with stats.lock:
                total_32 = stats.sent_count_32
                total_34 = stats.sent_count_34
            print(f"[运行状态] 已发送至 3.2: {total_32} 条 | 已发送至 3.4: {total_34} 条")

    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()

    print("[独居关怀系统] 模拟器启动 (PID: {})".format(os.getpid())) # 打印进程ID，确认是否重复启动
    print("正在加载微感知数据引擎...")
    print(f" 模拟速度: {SPEED_RATE}倍速")
    print("-" * 50)

    t1 = threading.Thread(target=process_file, args=(FILE_INT, True))
    t2 = threading.Thread(target=process_file, args=(FILE_FLOAT, False))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()

    print("[系统提示] 模拟数据发送完毕，系统保持待机")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[系统] 已退出")
