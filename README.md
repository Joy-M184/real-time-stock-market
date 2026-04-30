# 📈 Real-Time Stock Market Data Pipeline
Project Name: Real Time Stock Market Analysis

A production-grade, fully automated real-time data engineering pipeline that streams live stock market data from an API through Kafka, processes it with Apache Spark, stores it in PostgreSQL, and visualises it on a live Grafana dashboard — all running inside Docker.


## 🏗️ Architecture
![alt text](image-2.png)

Alpha Vantage API
      │
      ▼ (every 5 minutes)
┌─────────────┐
│  Producer   │  Fetches TSLA, MSFT, GOOGL intraday data
│  (Python)   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kafka    │  Message broker (KRaft mode, no Zookeeper)
│  (Topic:    │  stock_data topic
│ stock_data) │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Consumer   │  Reads from Kafka, inserts into PostgreSQL
│  (Python)   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ PostgreSQL  │  Stores raw stock data
│ (raw table) │
└──────┬──────┘
       │
       ▼ (every 5 minutes)
┌─────────────┐
│    Spark    │  Calculates moving averages, VWAP,
│  Analytics  │  price change %
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│    PostgreSQL    │  Stores enriched/processed data
│ (enriched table) │
└──────┬───────────┘
       │
       ▼
┌─────────────┐
│   Grafana   │  Live dashboard with charts and analytics
│  Dashboard  │
└─────────────┘
```


## Business Challenge
Customer Demand For More Insights: Clients are demanding more advanced analytics, such as predictive stock price movements, sentiment analysis, and portfolio performance optimization.

Scalability: As the volume of data grows, the existing infrastructure struggles to scale efficiently. This results in delays in delivering real-time insights to clients, particularly during periods of high market activity (e.g., market opening and closing hours, earnings reports).

Data Latency: The current system has occasional latency issues, especially when integrating data from multiple sources (e.g., stock exchanges, news feeds, and social media sentiment analysis). This affects the accuracy of the reports generated, which can harm client satisfaction and decision-making.

Project Objectives
Develop A Scalable Real-Time Data Pipeline: Implement a fault-tolerant, and scalable data pipeline using Kafka to stream stock market data from multiple exchanges, ensuring low latency and high availability.

Enhance Data Accuracy And Timeliness: Reduce latency and improve the accuracy of real-time data reports by streamlining the data processing workflow.

Build a Visualization Report: Develop a report using Power BI to visualize market trends, stock performance, and other financial metrics in real time.

Project Deliverable
The goal of this project is to ensure that MarketPulse Analytics is able to serve and meet its customers demand for updated stock market information while ensuring that it can stay ahead in the industry amid the industry competition
---

## 🚀 Tech Stack

| Component | Technology |
|---|---|
| Data Source | Alpha Vantage API (via RapidAPI) |
| Message Broker | Apache Kafka 7.4.10 (KRaft mode) |
| Stream Producer | Python + kafka-python |
| Stream Consumer | Python + psycopg2 |
| Data Processing | Apache Spark 3.5.1 |
| Database | PostgreSQL 16 |
| Visualisation | Grafana 10.4.0 |
| Containerisation | Docker + Docker Compose |
| DB Admin | pgAdmin 4 |
| Kafka UI | Kafka UI 0.7.2 |

---

## 📁 Project Structure

```
Producer/
├── main.py               # Entry point — orchestrates producer flow
├── extract.py            # API fetching and data extraction
├── producer_setup.py     # Kafka producer configuration
├── consumer.py           # Kafka consumer + PostgreSQL insertion
├── spark_job.py          # Spark analytics job
├── config.py             # Logging, API config, env vars
├── docker-compose.yml    # All services defined here
├── Dockerfile            # Python app container
├── requirements.txt      # Python dependencies
└── .env                  # Environment variables (not committed)
```

---

## ⚙️ Services

| Service | Port | Description |
|---|---|---|
| Kafka | 9092 (internal), 9094 (external) | Message broker |
| Kafka UI | 8085 | Visual Kafka management |
| PostgreSQL | 5434 (host), 5432 (internal) | Database |
| pgAdmin | 5050 | Database admin UI |
| Spark Master | 7077, 8081 | Spark cluster master |
| Spark Worker | — | Spark worker node |
| Grafana | 3000 | Live dashboard |

---

## 🛠️ Setup & Installation

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- An Alpha Vantage API key (via RapidAPI)

### 1. Clone the repository
```bash
git clone <https://github.com/Joy-M184/real-time-stock-market.git
git branch -M main
git push -u origin main>
cd Producer
```

### 2. Create your `.env` file
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=stock_data
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
API_KEY=your_rapidapi_key
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=stock_data
```

## CREATE AND ACTIVATE ENVIRONMENT
  python -m venv venv
  source venv/Scripts/activate

  ## INSTALL PROJECT DEPENDENCIES
    pip install -r requirements.txt

### 3. Start all services
```bash
docker compose up --build -d
```

### 4. Create the database table
```bash
docker exec -it postgres_db psql -U postgres -d stock_data -c "
CREATE TABLE IF NOT EXISTS public.stock_data (
    id         SERIAL PRIMARY KEY,
    date       TIMESTAMP,
    symbol     VARCHAR(10),
    open       NUMERIC,
    high       NUMERIC,
    low        NUMERIC,
    close      NUMERIC,
    volume     BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);"
```

### 5. Verify everything is running
```bash
docker compose ps
```

All containers should show **healthy** or **running**.

---

## 📊 Grafana Dashboard

1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Add PostgreSQL data source:
   - Host: `postgres:5432`
   - Database: `stock_data`
   - Username: `postgres`
   - TLS/SSL: `disable`
4. Create panels using the queries below

### Dashboard Queries

**Stock Close Price**
```sql
SELECT date AS "time", close::numeric AS value, symbol AS metric
FROM stock_data
WHERE $__timeFilter(date)
ORDER BY date ASC
```

**5-Period Moving Average**
```sql
SELECT date AS "time", moving_avg_5 AS value, symbol AS metric
FROM stock_data_enriched
WHERE $__timeFilter(date)
ORDER BY date ASC
```

**Price Change %**
```sql
SELECT date AS "time", price_change_pct AS value, symbol AS metric
FROM stock_data_enriched
WHERE $__timeFilter(date)
ORDER BY date ASC
```

**Trading Volume**
```sql
SELECT date AS "time", volume AS value, symbol AS metric
FROM stock_data
WHERE $__timeFilter(date)
ORDER BY date ASC
```

---

## 🔄 How It Works

1. **Producer** runs every 5 minutes inside Docker, fetching the latest intraday data for TSLA, MSFT, and GOOGL from the Alpha Vantage API and publishing each record to the `stock_data` Kafka topic.

2. **Consumer** listens continuously to the Kafka topic and inserts every incoming message into the `stock_data` PostgreSQL table.

3. **Spark Job** runs every 5 minutes, reads the raw data from PostgreSQL, calculates analytics (5-period moving average, VWAP, price change %), and writes the enriched results to the `stock_data_enriched` table.

4. **Grafana** queries both tables in real time and renders the live dashboard.

---

## 📈 Spark Analytics

The Spark job calculates the following indicators:

| Indicator | Description |
|---|---|
| `moving_avg_5` | 5-period rolling average of close price |
| `vwap` | Volume Weighted Average Price |
| `price_change_pct` | Percentage change from open to close |

---

## 🐛 Troubleshooting

**Consumer keeps restarting**
- Check `.env` — ensure `POSTGRES_HOST=postgres` (not `localhost`)
- Check `.env` — ensure `POSTGRES_PORT=5432` (not `5434`)
- Check `.env` — ensure `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`

**No data in PostgreSQL**
- Verify the `stock_data` table exists: `docker exec -it postgres_db psql -U postgres -d stock_data -c "\dt"`
- Check consumer logs: `docker compose logs consumer --tail=30`

**Spark job failing**
- Check logs: `docker compose logs spark-job --tail=30`
- Ensure spark-master is healthy: `docker compose ps spark-master`

---
## SHUT DOWN SERVER
---

## 👤 Author

Built by Joy Michael— a real-time data engineering pipeline project.

---