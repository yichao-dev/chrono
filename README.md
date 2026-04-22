# 智慧社区网格实时监控矩阵

## 项目简介
`grid_monitor.html` 是一个基于 Vue 和 ECharts 构建的智慧社区网格实时监控系统，用于展示老人实时状态、亲情日报和 AI 智能问答功能。

## 功能特点
- 老人实时状态监控
- 亲情日报展示
- AI 智能问答
- 报警处理时间轴
- 响应式布局

## 如何使用
1. 直接在浏览器中打开 `grid_monitor.html` 文件
2. 确保后端 API 服务已启动（默认配置：`http://10.40.156.213:8000` 和 `http://10.40.156.146:5000`）
3. 点击老人卡片查看详细报警信息
4. 在 AI 智能问答区域输入问题获取回答

## 依赖项
- [Vue 3](https://cdn.jsdelivr.net/npm/vue@3/dist/vue.global.js)
- [ECharts 5](https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js)

## API 接口
- `GET /api/elders/status` - 获取老人状态
- `GET /api/alert/{alert_id}/timeline` - 获取报警时间轴
- `GET /api/get_result` - 获取亲情日报数据
- `POST /api/ai_chat` - AI 智能问答

## 注意事项
- 请根据实际部署环境修改 `mainApiUrl` 和 `dailyReportApiUrl` 配置
- 该项目使用 CDN 加载依赖，需要网络连接
