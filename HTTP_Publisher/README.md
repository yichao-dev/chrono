1. 核心功能
数据回放：支持读取 `int` (数字信号) 和 `float` (模拟信号) 两种格式的 CSV 数据。
变速播放：支持调节 `SPEED_RATE`，可极速回放历史数据，也可实时模拟。
HTTP 推送：将结构化数据通过 POST 请求发送至指定后端接口。

2. 环境依赖
Python 3.7+
依赖库：`pandas`, `requests`

安装依赖：
pip install pandas requests
