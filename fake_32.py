from flask import Flask, request

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    data = request.json
    print(f"🤖 [3.2 模块] 收到数据: 设备 {data.get('device_id')} 数值 {data.get('value')}")
    return "OK"

if __name__ == '__main__':
    print("🟢 模拟 3.2 模块启动，监听 5001 端口...")
    app.run(port=5001)