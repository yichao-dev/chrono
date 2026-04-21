import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Deque

import numpy as np
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from sklearn.ensemble import IsolationForest
import httpx

# ---------- 配置 ----------
SERVICE_HOST = "0.0.0.0"
SERVICE_PORT = 8001
DOWNSTREAM_URL = "http://10.40.156.213:8000/api/alert/from-detection"   # 3.3 模块地址
MAX_HISTORY_DAYS = 7                     # 保留最近7天原始数据用于训练
FEATURE_WINDOW_MINUTES = 60              # 特征提取窗口长度（分钟）
RED_NO_ACTIVITY_HOURS = 2               # 红色预警：连续无活动小时数
YELLOW_ISOLATION_CONTAMINATION = 0.1     # IsolationForest 异常比例
YELLOW_CONFIDENCE_THRESHOLD = 0.7        # 黄色预警最低置信度
COOLDOWN_RED_SECONDS = 10               # 红色预警冷却时间（10分钟）
COOLDOWN_YELLOW_SECONDS = 30           # 黄色预警冷却时间（30分钟）
MODEL_RETRAIN_HOURS = 24                 # 每24小时重新训练模型

# ---------- Pydantic 模型（与 3.1 模块对齐）----------
class SensorDataItem(BaseModel):
    sensor_id: str
    node_id: str
    timestamp: str
    value: float
    data_type: str          # "INT" 或 "FLOAT"
    location: str
    function: str           # "motion" 或 "current"

class Module31Request(BaseModel):
    request_type: str
    timestamp: str
    source: str
    data: List[SensorDataItem]

# ---------- 下游报警模型（与 3.3 模块对齐）----------
class AlertItem(BaseModel):
    alert_id: str
    alert_level: str          # "RED" / "YELLOW"
    sensor_id: str
    sensor_name: str
    anomaly_type: str
    timestamp: str
    detected_value: float
    baseline_value: float
    confidence: float
    description: str
    recommendations: List[str]

class DownstreamRequest(BaseModel):
    status: int
    message: str
    timestamp: str
    alerts: List[AlertItem]

# ---------- 传感器上下文 ----------
class SensorContext:
    """每个传感器的运行时状态"""
    def __init__(self, sensor_id: str, sensor_type: str, function: str):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type   # "INT" 或 "FLOAT"
        self.function = function         # "motion" 或 "current"
        self.history: Deque[tuple] = deque()   # 元素为 (timestamp, value)
        self.last_red_alert_time: float = 0
        self.last_yellow_alert_time: float = 0
        self.model: Optional[IsolationForest] = None
        self.last_train_time: float = 0
        self.feature_cache: Deque[Dict] = deque(maxlen=MAX_HISTORY_DAYS * 24)  # 每小时一个特征点

    def add_data_point(self, ts: datetime, value: float):
        """添加原始数据点，保留 MAX_HISTORY_DAYS 天"""
        self.history.append((ts, value))
        # 清理过期数据（保留最近 MAX_HISTORY_DAYS 天）
        cutoff = datetime.now() - timedelta(days=MAX_HISTORY_DAYS)
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    def extract_features(self, ts: datetime) -> Optional[Dict]:
        """
        提取当前时间窗口（过去 FEATURE_WINDOW_MINUTES 分钟）的特征。
        返回特征字典，若数据不足则返回 None。
        """
        window_start = ts - timedelta(minutes=FEATURE_WINDOW_MINUTES)
        window_data = [(t, v) for t, v in self.history if window_start <= t <= ts]
        if not window_data:
            return None

        values = [v for _, v in window_data]
        if self.sensor_type == "INT" and self.function == "motion":
            # 运动传感器：统计开关次数、开启次数占比等
            motion_count = sum(1 for v in values if v == 1)
            total_events = len(values)
            feature = {
                "hour": ts.hour,
                "total_events": total_events,
                "motion_count": motion_count,
                "motion_ratio": motion_count / total_events if total_events > 0 else 0,
                "value_std": np.std(values) if len(values) > 1 else 0,
            }
        else:
            # 连续值传感器（用水、电流）：统计均值、总和、方差等
            feature = {
                "hour": ts.hour,
                "total_events": len(values),
                "value_sum": np.sum(values),
                "value_mean": np.mean(values),
                "value_std": np.std(values) if len(values) > 1 else 0,
            }
        return feature

    def train_model(self):
        """使用历史特征训练 IsolationForest 模型"""
        if len(self.feature_cache) < 24:   # 至少需要一天的数据才能训练
            return
        df = pd.DataFrame(self.feature_cache)
        # 选择数值列作为训练特征（排除 hour）
        feature_cols = [c for c in df.columns if c != "hour"]
        X = df[feature_cols].values
        if X.shape[0] < 10:
            return
        self.model = IsolationForest(
            contamination=YELLOW_ISOLATION_CONTAMINATION,
            random_state=42,
            n_estimators=100
        )
        self.model.fit(X)
        self.last_train_time = time.time()

    def predict_anomaly(self, feature: Dict) -> tuple[bool, float]:
        """
        使用 IsolationForest 预测当前特征是否异常。
        返回 (是否异常, 置信度) ，置信度 = 1 - 异常分数（归一化到0~1）
        """
        if self.model is None:
            return False, 0.0
        # 构造特征向量（顺序与训练时一致）
        feature_cols = [c for c in self.feature_cache[0].keys() if c != "hour"]
        X = np.array([[feature[c] for c in feature_cols]])
        # 预测：1 表示正常，-1 表示异常
        pred = self.model.predict(X)[0]
        # 获取异常分数（负数越大表示越异常）
        score = self.model.score_samples(X)[0]   # 越高越正常
        # 将分数映射到置信度（异常程度）
        # score 范围通常在 [-0.5, 0.5] 之间，映射到 [0,1]
        confidence = 1.0 / (1.0 + np.exp(-score * 5))   # sigmoid 变换
        if pred == -1:
            # 异常，返回置信度（异常的可信度）
            return True, confidence
        else:
            return False, 1.0 - confidence

# ---------- 全局存储 ----------
sensors: Dict[str, SensorContext] = {}
# 线程锁（asyncio 中使用 asyncio.Lock）
sensor_lock = asyncio.Lock()

# ---------- 辅助函数 ----------
def generate_alert_id() -> str:
    return f"alert_{int(time.time() * 1000)}_{np.random.randint(1000, 9999)}"

async def send_alert_to_downstream(alert: AlertItem):
    """发送预警到 3.3 模块（非阻塞）"""
    async with httpx.AsyncClient(timeout=5.0) as client:
        payload = DownstreamRequest(
            status=0,
            message="success",
            timestamp=datetime.now().isoformat(),
            alerts=[alert]
        )
        try:
            resp = await client.post(DOWNSTREAM_URL, json=payload.dict())
            if resp.status_code == 200:
                print(f"[发送成功] {alert.alert_id} -> {DOWNSTREAM_URL}")
            else:
                print(f"[发送失败] HTTP {resp.status_code} : {resp.text}")
        except Exception as e:
            print(f"[发送异常] {alert.alert_id} : {str(e)}")

async def rule_based_detection(ctx: SensorContext, now: datetime) -> Optional[AlertItem]:
    """规则引擎：连续 RED_NO_ACTIVITY_HOURS 小时无活动 -> 红色预警"""
    # 检查历史数据中最近 RED_NO_ACTIVITY_HOURS 小时内是否有数据
    cutoff = now - timedelta(hours=RED_NO_ACTIVITY_HOURS)
    recent_data = [(t, v) for t, v in ctx.history if t >= cutoff]
    if not recent_data:
        # 没有任何数据点，视为无活动
        description = f"传感器 {ctx.sensor_id} 已连续 {RED_NO_ACTIVITY_HOURS} 小时无任何数据上报，可能老人长时间无活动。"
        return AlertItem(
            alert_id=generate_alert_id(),
            alert_level="RED",
            sensor_id=ctx.sensor_id,
            sensor_name=f"传感器_{ctx.sensor_id}",
            anomaly_type="长期无活动",
            timestamp=now.isoformat(),
            detected_value=0.0,
            baseline_value=1.0,
            confidence=0.95,
            description=description,
            recommendations=["请立即联系家属或上门查看"]
        )
    # 对于运动传感器，检查是否有开启（值为1）的事件
    if ctx.sensor_type == "INT" and ctx.function == "motion":
        has_activity = any(v == 1 for _, v in recent_data)
        if not has_activity:
            description = f"运动传感器 {ctx.sensor_id} 在过去 {RED_NO_ACTIVITY_HOURS} 小时内未检测到任何活动（无开启事件）。"
            return AlertItem(
                alert_id=generate_alert_id(),
                alert_level="RED",
                sensor_id=ctx.sensor_id,
                sensor_name=f"传感器_{ctx.sensor_id}",
                anomaly_type="长期无活动",
                timestamp=now.isoformat(),
                detected_value=0.0,
                baseline_value=1.0,
                confidence=0.92,
                description=description,
                recommendations=["请立即联系家属或社区网格员"]
            )
    return None

async def ml_based_detection(ctx: SensorContext, now: datetime) -> Optional[AlertItem]:
    """机器学习基线偏离检测：返回黄色预警（若异常）"""
    # 提取当前小时的特征
    feature = ctx.extract_features(now)
    if feature is None:
        return None
    # 将特征加入缓存（用于后续训练）
    ctx.feature_cache.append(feature)

    # 定期重新训练模型
    if time.time() - ctx.last_train_time > MODEL_RETRAIN_HOURS * 3600:
        ctx.train_model()

    # 预测异常
    is_anomaly, confidence = ctx.predict_anomaly(feature)
    if is_anomaly and confidence >= YELLOW_CONFIDENCE_THRESHOLD:
        # 构造描述信息
        if ctx.sensor_type == "INT" and ctx.function == "motion":
            description = f"运动传感器活动模式偏离日常基线（活动次数异常低）。当前窗口活动次数={feature.get('motion_count',0)}，历史同时段均值较低。"
        else:
            description = f"连续值传感器数据偏离日常基线（总量或均值异常）。当前窗口均值={feature.get('value_mean',0):.2f}，与历史模式不符。"
        return AlertItem(
            alert_id=generate_alert_id(),
            alert_level="YELLOW",
            sensor_id=ctx.sensor_id,
            sensor_name=f"传感器_{ctx.sensor_id}",
            anomaly_type="行为基线偏离",
            timestamp=now.isoformat(),
            detected_value=float(feature.get('value_sum', 0)),
            baseline_value=0.0,   # 具体基线值省略
            confidence=confidence,
            description=description,
            recommendations=["请通过小程序查看详情，或联系老人确认状况"]
        )
    return None

async def process_sensor_data(sensor_id: str, ts: datetime, value: float, data_type: str, function: str):
    """处理单条传感器数据：更新历史、触发检测（带冷却）"""
    async with sensor_lock:
        if sensor_id not in sensors:
            sensors[sensor_id] = SensorContext(sensor_id, data_type, function)
        ctx = sensors[sensor_id]

    ctx.add_data_point(ts, value)

    now = datetime.now()
    # 1. 规则检测（红色预警）
    red_alert = await rule_based_detection(ctx, now)
    if red_alert:
        # 冷却检查
        if time.time() - ctx.last_red_alert_time >= COOLDOWN_RED_SECONDS:
            ctx.last_red_alert_time = time.time()
            await send_alert_to_downstream(red_alert)
        return  # 红色优先级最高，不再检查黄色

    # 2. 机器学习检测（黄色预警）
    yellow_alert = await ml_based_detection(ctx, now)
    if yellow_alert:
        if time.time() - ctx.last_yellow_alert_time >= COOLDOWN_YELLOW_SECONDS:
            ctx.last_yellow_alert_time = time.time()
            await send_alert_to_downstream(yellow_alert)

# ---------- FastAPI 应用 ----------
app = FastAPI(title="时序守望 - 渐进式时序异常检测中枢")

@app.post("/api/v1/3_2/sensor_data")
async def receive_sensor_data(request: Module31Request, background_tasks: BackgroundTasks):
    """
    接收 3.1 模块推送的传感器数据，进行异常检测，并将预警异步发送至 3.3 模块。
    """
    for item in request.data:
        try:
            ts = datetime.fromisoformat(item.timestamp.replace('Z', '+00:00'))
        except Exception:
            ts = datetime.now()
        background_tasks.add_task(
            process_sensor_data,
            item.sensor_id,
            ts,
            item.value,
            item.data_type,
            item.function
        )
    return {"code": 0, "message": f"已接收 {len(request.data)} 条数据"}

@app.get("/health")
async def health_check():
    return {"status": "alive", "sensors_count": len(sensors)}

# ---------- 启动服务 ----------
if __name__ == "__main__":
    import uvicorn
    print("[3.2 算法引擎] 启动中...")
    print(f"监听地址: {SERVICE_HOST}:{SERVICE_PORT}")
    print(f"下游模块地址: {DOWNSTREAM_URL}")
    uvicorn.run(app, host=SERVICE_HOST, port=SERVICE_PORT)