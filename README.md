# 🌤️ Weather Forecast ML Project

## Tổng quan dự án

Hệ thống dự báo thời tiết tự động sử dụng Machine Learning (Prophet), Apache Airflow cho orchestration, và Telegram Bot để gửi thông báo hàng ngày.

### ⚙️ Kiến trúc hệ thống

```
Open-Meteo API
      ↓
Airflow (ETL Pipeline)
      ↓
PostgreSQL Database
      ↓
Prophet ML Model
      ↓
Telegram Bot
```

### 🎯 Tính năng chính

- ✅ Thu thập dữ liệu thời tiết mỗi giờ từ Open-Meteo API
- ✅ ETL pipeline với feature engineering
- ✅ Lưu trữ dữ liệu trong PostgreSQL
- ✅ Huấn luyện model Prophet tự động
- ✅ Dự báo thời tiết 24 giờ tiếp theo
- ✅ Gửi dự báo hàng ngày qua Telegram Bot

---

## 📋 Yêu cầu hệ thống

- Docker & Docker Compose
- 4GB RAM tối thiểu
- 10GB disk space
- Kết nối internet

---

## 🚀 Hướng dẫn cài đặt

### Bước 1: Clone project

```bash
git clone <your-repo>
cd weather-forecast-project
```

### Bước 2: Tạo Telegram Bot

1. Mở Telegram, tìm **@BotFather**
2. Gửi lệnh: `/newbot`
3. Đặt tên bot và username (phải kết thúc bằng `_bot`)
4. Copy **BOT TOKEN** được tạo ra

### Bước 3: Lấy Group Chat ID

1. Tạo group Telegram mới
2. Thêm bot vào group
3. Gửi tin nhắn bất kỳ trong group
4. Truy cập:
   ```
   https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
   ```
5. Tìm `"chat":{"id":-1001234567890}` → Copy số này

### Bước 4: Cấu hình environment variables

Tạo file `.env` trong thư mục gốc:

```bash
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=weather_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/weather_db
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Telegram Bot (THAY ĐỔI NHỮNG GIÁ TRỊ NÀY!)
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_GROUP_CHAT_ID=-1001234567890
```

⚠️ **QUAN TRỌNG**: Thay thế `TELEGRAM_BOT_TOKEN` và `TELEGRAM_GROUP_CHAT_ID` bằng giá trị thực của bạn!

### Bước 5: Tạo cấu trúc thư mục

```bash
mkdir -p airflow/dags
mkdir -p airflow/plugins
mkdir -p airflow/logs
mkdir -p database
mkdir -p models
mkdir -p config
mkdir -p tests
```

### Bước 6: Copy các file code

Copy tất cả các file từ artifacts vào đúng thư mục:

```
weather-forecast-project/
├── docker-compose.yml
├── .env
├── requirements.txt
├── README.md
├── airflow/
│   ├── dags/
│   │   ├── weather_ingestion_dag.py
│   │   └── weather_ml_forecast_dag.py
│   └── plugins/
│       ├── weather_api.py
│       ├── data_transformer.py
│       ├── ml_model.py
│       └── telegram_bot.py
├── database/
│   └── init.sql
└── config/
    └── locations.json
```

### Bước 7: Khởi động hệ thống

```bash
# Khởi động Docker containers
docker-compose up -d

# Xem logs
docker-compose logs -f

# Kiểm tra status
docker-compose ps
```

### Bước 8: Truy cập Airflow

1. Mở browser: http://localhost:8080
2. Login:
   - Username: `admin`
   - Password: `admin`
3. Bật 2 DAGs:
   - `weather_ingestion`
   - `weather_ml_forecast`

---

## 📊 Kiểm tra hệ thống

### Test Telegram Bot

```bash
docker exec -it weather_airflow_webserver python3 << EOF
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from telegram_bot import TelegramWeatherBot

bot = TelegramWeatherBot()
bot.test_connection()
bot.send_message("🎉 Bot đã hoạt động!")
EOF
```

### Xem database

1. Mở browser: http://localhost:5050
2. Login:
   - Email: `admin@admin.com`
   - Password: `admin`
3. Kết nối đến PostgreSQL:
   - Host: `postgres`
   - Database: `weather_db`
   - Username: `airflow`
   - Password: `airflow`

### Chạy thử DAG

Trong Airflow UI:
1. Click vào DAG `weather_ingestion`
2. Click nút **Trigger DAG** (▶️)
3. Theo dõi các task trong Graph View

---

## 📅 Lịch chạy tự động

| DAG | Schedule | Mô tả |
|-----|----------|-------|
| `weather_ingestion` | Mỗi giờ (0 * * * *) | Thu thập dữ liệu thời tiết |
| `weather_ml_forecast` | 6h chiều hàng ngày (0 18 * * *) | Train model + gửi Telegram |

---

## 🔍 Troubleshooting

### Lỗi: "No module named 'prophet'"

```bash
docker exec -it weather_airflow_scheduler pip install prophet
docker-compose restart airflow-scheduler
```

### Lỗi: "Telegram bot connection failed"

Kiểm tra:
1. `TELEGRAM_BOT_TOKEN` đúng chưa?
2. Bot đã được thêm vào group chưa?
3. `TELEGRAM_GROUP_CHAT_ID` có dấu `-` ở đầu không?

### Lỗi: "No training data available"

1. Đợi DAG `weather_ingestion` chạy ít nhất 1 lần
2. Kiểm tra database có dữ liệu chưa:
   ```sql
   SELECT COUNT(*) FROM hourly_weather;
   ```

### Airflow không khởi động

```bash
# Xem logs
docker-compose logs airflow-webserver

# Restart
docker-compose restart

# Rebuild nếu cần
docker-compose down
docker-compose up --build -d
```

---

## 📈 Monitoring & Logs

### Xem logs Airflow

```bash
# Webserver logs
docker-compose logs -f airflow-webserver

# Scheduler logs
docker-compose logs -f airflow-scheduler

# All logs
docker-compose logs -f
```

### Query database

```sql
-- Xem current weather
SELECT * FROM v_latest_weather;

-- Xem model accuracy
SELECT * FROM v_model_accuracy;

-- Xem ETL job history
SELECT * FROM etl_job_logs ORDER BY started_at DESC LIMIT 10;

-- Xem predictions
SELECT 
    l.name,
    wp.prediction_timestamp,
    wp.predicted_temperature,
    wp.actual_temperature,
    wp.error
FROM weather_predictions wp
JOIN locations l ON wp.location_id = l.location_id
ORDER BY wp.prediction_timestamp DESC
LIMIT 20;
```

---

## 🎓 Cấu trúc để báo cáo/thuyết trình

### Slide 1: Tổng quan dự án
- Mục tiêu: Dự báo thời tiết tự động
- Tech stack: Airflow + Prophet + Telegram
- Data source: Open-Meteo API (free)

### Slide 2: Kiến trúc hệ thống
- Diagram: API → Airflow → PostgreSQL → ML → Telegram
- Giải thích từng component

### Slide 3: ETL Pipeline
- Extract: Open-Meteo API
- Transform: Feature engineering (20+ features)
- Load: PostgreSQL với 7 tables

### Slide 4: Feature Engineering
- Time features: hour, day_of_week, cyclical encoding
- Lag features: temp_lag_1h, temp_lag_3h, temp_lag_24h
- Rolling statistics: moving average, std
- Weather categories

### Slide 5: Machine Learning Model
- Algorithm: Facebook Prophet
- Training: Hàng ngày với 30 ngày data
- Metrics: MAE, RMSE, R²
- Confidence interval: 95%

### Slide 6: Automation
- Airflow DAGs schedule
- Error handling & retry
- Monitoring & alerting

### Slide 7: Results & Demo
- Show Telegram bot message
- Show Airflow UI
- Show database queries
- Show metrics

### Slide 8: Tương lai
- Thêm thành phố
- Thêm weather alerts
- Improve model accuracy
- Add web dashboard

---

## 🛠 Customization

### Thêm thành phố

Edit `config/locations.json`:

```json
{
  "cities": [
    {
      "id": 4,
      "name": "Nha Trang",
      "latitude": 12.2388,
      "longitude": 109.1967,
      "timezone": "Asia/Ho_Chi_Minh"
    }
  ]
}
```

### Thay đổi schedule

Edit DAG files:
- `weather_ingestion_dag.py`: line `schedule_interval='0 * * * *'`
- `weather_ml_forecast_dag.py`: line `schedule_interval='0 18 * * *'`

### Customize Telegram message

Edit `airflow/plugins/telegram_bot.py`:
- Function `format_forecast_message()`

---

## 📚 References

- [Open-Meteo API](https://open-meteo.com/en/docs)
- [Apache Airflow](https://airflow.apache.org/)
- [Prophet Documentation](https://facebook.github.io/prophet/)
- [Telegram Bot API](https://core.telegram.org/bots/api)

---

## 👥 Contributors

- Your Name - Data Engineer
- Project: Weather Forecast ML System

---

## 📝 License

MIT License

---

## 🎉 Kết luận

Bây giờ bạn có một hệ thống hoàn chỉnh:
- ✅ Data Engineering (ETL)
- ✅ Machine Learning (Prophet)
- ✅ Automation (Airflow)
- ✅ Real-time notification (Telegram)
- ✅ Monitoring & Logging

**Chúc bạn demo thành công! 🚀**

