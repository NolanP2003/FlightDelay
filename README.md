# FlightDelay
<img src="https://img.shields.io/badge/FlightDelay-Real--Time%20Prediction%20System-blue?style=for-the-badge" alt="FlightDelay Badge">

# ![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white) ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) ![Flask](https://img.shields.io/badge/Flask-000000?style=for-the-badge&logo=flask&logoColor=white)

**FlightDelay** is a real-time flight delay prediction system that uses streaming data processing and machine learning to forecast severe delays (≥60 minutes) before they occur. The system ingests live flight data through Apache Kafka, processes it with Apache Spark Structured Streaming, and applies trained Gradient Boosted Tree models to deliver actionable predictions via an interactive web dashboard.

---

## 📚 Documentation

For detailed guides, technical info, and step-by-step instructions, visit our wiki:

- [Home](https://github.com/NolanP2003/FlightDelay/wiki) – Project overview, motivation, and key features
- [Core Features](https://github.com/NolanP2003/FlightDelay/wiki/Core-Features) – Real-time streaming, ML models, and visualizations
- [Installation & Setup](https://github.com/NolanP2003/FlightDelay/wiki/Installation-&-Setup) – Complete local and GCP deployment guide
- [Tech Stack](https://github.com/NolanP2003/FlightDelay/wiki/Tech-Stack) – Big data tools, ML frameworks, and libraries
- [Project Structure](https://github.com/NolanP2003/FlightDelay/wiki/Project-Structure) – File organization and architecture
- [Our Team](https://github.com/NolanP2003/FlightDelay/wiki/Our-Team) – Meet the developers behind FlightDelay

---

## Prerequisites

Before starting, ensure you have:
- **Python 3.8+** – [Download Python](https://www.python.org/downloads/)
- **Apache Kafka 2.13** – [Download Kafka](https://kafka.apache.org/downloads)
- **Apache Spark 3.5.1** – [Download Spark](https://spark.apache.org/downloads.html)
- **Jupyter Lab** – [Install Jupyter](https://jupyter.org/install)
- **GCP Account** (for VM deployment) or local machine with **16GB+ RAM**
- **Dataset**: `flight_data.csv` (historical flight records)

**Optional:**
- [VS Code](https://code.visualstudio.com/) with [Live Server Extension](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer)
- [MongoDB](https://www.mongodb.com/try/download/community) (if using MongoDB storage)

---

## Quick Setup

### 1. Clone & Navigate
```bash
git clone https://github.com/NolanP2003/FlightDelay.git
cd FlightDelay
```

### 2. Install Python Dependencies
```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install pyspark==3.5.1 pandas numpy matplotlib seaborn kafka-python flask findspark
```

**Verify installation:**
```bash
python3 -c "import pyspark; print(pyspark.__version__)"
# Should output: 3.5.1
```

### 3. Set Up Apache Kafka

**On Linux/Ubuntu:**
```bash
# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

# Start Zookeeper (Terminal 1)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Server (Terminal 2)
bin/kafka-server-start.sh config/server.properties
```

**On macOS (with Homebrew):**
```bash
brew install kafka
brew services start zookeeper
brew services start kafka
```

**Verify Kafka is running:**
```bash
sudo systemctl status kafka
# Should show: active (running)
```

### 4. Set Up Apache Spark

**Download and configure Spark:**
```bash
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
```

**Add to your PATH** (in `~/.bashrc` or `~/.zshrc`):
```bash
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
```

**Apply changes:**
```bash
source ~/.bashrc  # or source ~/.zshrc
```

### 5. Prepare Dataset

1. Place your `flight_data.csv` file in the project root directory
2. The dataset should include columns:
   - `FL_DATE`, `AIRLINE`, `AIRLINE_CODE`, `ORIGIN`, `DEST`
   - `CRS_DEP_TIME`, `CRS_ARR_TIME`, `CRS_ELAPSED_TIME`
   - `DEP_TIME`, `ARR_TIME`, `DEP_DELAY`, `ARR_DELAY`
   - `DISTANCE`, `CANCELLED`, `DIVERTED`
   - Delay cause columns (`DELAY_DUE_CARRIER`, etc.)

**Verify dataset location:**
```bash
ls -lh flight_data.csv
# Should show file size (typically 100MB - 2GB)
```

⚠️ **IMPORTANT**: The CSV file is NOT included in the repository. You must provide your own flight dataset.

### 6. Start Jupyter Lab
```bash
jupyter lab
# Server will start at http://localhost:8888
```

Your browser should automatically open. If not, copy the URL from the terminal.

### 7. Train Models (One-Time Setup)

This step must be completed before streaming predictions.

**Step 1: Data Visualization (Optional)**
1. In Jupyter Lab, open `Step1_Data_Visualization.ipynb`
2. Click **Run All Cells** (Cell → Run All)
3. This performs EDA and generates visualizations
4. **Time required:** ~10-15 minutes

**Step 2: Batch Processing (Required)**
1. In Jupyter Lab, open `Step2_Batch_Processing.ipynb`
2. Click **Run All Cells** (Cell → Run All)
3. This will:
   - Clean and preprocess the dataset
   - Engineer features
   - Train GBT, Random Forest, and Logistic Regression models
   - Evaluate models with confusion matrices
   - **Save trained models to disk**
4. **Time required:** ~20-30 minutes

**Verify models were saved:**
```bash
ls -lh flight_delay_gbt_pipeline_model/
ls -lh flight_delay_gbt_model/
# Both directories should contain Spark model files
```

⚠️ **CRITICAL**: Do not skip Step 2. The streaming pipeline requires these saved models.

---

## Start Real-Time Streaming Pipeline

You'll run three processes simultaneously: **Kafka Producer**, **Spark Streaming**, and **Flask Dashboard**.

### Terminal 1 - Start Kafka Producer

From the project directory:
```bash
python3 kafka_producer.py --csv flight_data.csv --speed 100
```

**Command-line options:**
- `--csv`: Path to CSV file (default: `flight_data.csv`)
- `--topic`: Kafka topic name (default: `flight_data_stream`)
- `--servers`: Kafka bootstrap servers (default: `localhost:9092`)
- `--speed`: Records per second (default: 100, use 0 for max speed)

**Expected output:**
```
Starting to stream data from flight_data.csv to Kafka topic 'flight_data_stream'...
CSV Headers: ['FL_DATE', 'AIRLINE', 'AIRLINE_CODE', ...]
Sent 1000 records...
Sent 2000 records...
```

**Keep this terminal running.** The producer continuously sends flight data to Kafka.

### Terminal 2 (Jupyter Lab) - Start Spark Streaming

In Jupyter Lab:
1. Open `Step3_Streaming_Prediction.ipynb`
2. Click **Run All Cells** (Cell → Run All)
3. The notebook will:
   - Load saved preprocessing pipeline and GBT model
   - Connect to Kafka topic `flight_data_stream`
   - Process streaming data in real-time
   - Generate predictions for each flight
   - Save predictions to Parquet files

**Expected output:**
```
-------------------------------------------
Batch: 0
-------------------------------------------
+----------+------------+------+-----+------------+--------------------+-------------------------+
|FL_DATE   |AIRLINE_CODE|ORIGIN|DEST |CRS_DEP_TIME|Prediction_Label    |Probability_Severe_Delay |
+----------+------------+------+-----+------------+--------------------+-------------------------+
|2024-01-15|AA          |LAX   |JFK  |800         |Severe Delay Pred...|0.78                     |
|2024-01-15|UA          |ORD   |SFO  |1430        |No Severe Delay P...|0.12                     |
+----------+------------+------+-----+------------+--------------------+-------------------------+
```

**Keep this notebook running.** It continuously processes streaming data.

### Terminal 3 - Start Flask Dashboard

Open a new terminal and run:
```bash
cd FlightDelay
python3 app.py
```

**Expected output:**
```
🚀 Flask app started!
 * Running on http://0.0.0.0:5000
 * Debug mode: on
```

### Access the Dashboard

**Local Machine:**
```
http://localhost:5000
```

**GCP VM:**
```
http://<YOUR_VM_EXTERNAL_IP>:5000
```

To find your GCP VM's external IP:
```bash
curl ifconfig.me
```

The dashboard auto-refreshes every 10 seconds and displays:
- Top 50 most recent flight predictions
- Flight details (date, airline, origin, destination, scheduled departure)
- Prediction label (Severe Delay Predicted vs No Severe Delay)
- Probability of severe delay (percentage)

---

## View Dataset Visualizations (Local Machine)

The Dataset Application is a separate static web app for exploring pre-generated visualizations.

### Setup

1. **On your local machine** (not on GCP VM), open VS Code
2. Open the `FlightDelay` folder in VS Code
3. Ensure **Live Server extension** by Ritwick Dey is installed:
   - Extensions → Search "Live Server" → Install

### Launch Visualization App

1. In VS Code Explorer, navigate to: `Dataset Application/index.html`
2. Right-click `index.html` → Select **"Open with Live Server"**
3. Your browser will open to: `http://127.0.0.1:5500/Dataset%20Application/index.html`

### Using the Visualization Matrix

**7×5 Grid (Dataset Visualizations):**
- **Columns:** Select a category (Day of Week, Month, Hour, Origin Airport, Dest Airport, Airline, Cause of Delay)
- **Rows:** Select a metric (Average Arrival Delay, Number of Flights, Severe Delays, Proportion by Severity, Ratio of Flight Time/Delay)
- **Image Updates:** Click any row + column to see corresponding visualization

**3D Matrix (Model Evaluations):**
- **Dimension 1:** Confusion Matrix or Model Evaluation
- **Dimension 2:** Model Type (RFC, LR, GBT)
- **Dimension 3:** Class Balancing (Weighting or Resampling)
- **Result:** View confusion matrices and evaluation metrics for all model combinations

---

## Alternative Setup

### Backend Only (Streaming Pipeline)
```bash
# Terminal 1: Kafka Producer
python3 kafka_producer.py --csv flight_data.csv --speed 100

# Terminal 2: Jupyter Lab
jupyter lab
# Then run Step3_Streaming_Prediction.ipynb

# Terminal 3: Flask Dashboard
python3 app.py
```

### Visualization App Only
```bash
# Open Dataset Application/index.html in VS Code
# Right-click → "Open with Live Server"
```

---

## Verify Installation

### 1. Check All Services Are Running

**Kafka:**
```bash
sudo systemctl status kafka
# Should show: active (running)
```

**Jupyter Lab:**
- Open browser to `http://localhost:8888`
- You should see the Jupyter Lab interface

**Kafka Producer:**
- Terminal 1 should show "Sent X records..." messages

**Spark Streaming:**
- Jupyter notebook should display batch predictions

**Flask Dashboard:**
```bash
curl http://localhost:5000
# Should return HTML content
```

### 2. Test the Prediction Dashboard

1. Open browser to `http://localhost:5000` (or `http://<VM_IP>:5000`)
2. You should see the FlightDelay dashboard
3. The table should populate with predictions (may take 30-60 seconds for first batch)
4. Verify:
   - Flight details are displayed correctly
   - Prediction labels show "Severe Delay Predicted" or "No Severe Delay Predicted"
   - Probability percentages are displayed
   - Page auto-refreshes every 10 seconds

### 3. Test the Dataset Application

1. Open `Dataset Application/index.html` with Live Server
2. You should see the visualization matrix interface
3. Click any row button and any column button
4. An image should appear showing the selected visualization
5. Try the 3D matrix section (model evaluations)

---

## Project Workflow Summary

1. **Data Exploration** → Run `Step1_Data_Visualization.ipynb`
2. **Model Training** → Run `Step2_Batch_Processing.ipynb` (saves models)
3. **Start Streaming** → Run `kafka_producer.py` (Terminal 1)
4. **Start Predictions** → Run `Step3_Streaming_Prediction.ipynb` (Jupyter)
5. **Start Dashboard** → Run `app.py` (Terminal 2)
6. **View Predictions** → Access `http://<VM_IP>:5000`
7. **Explore Visualizations** → Open `Dataset Application/index.html` (local machine)

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **Apache Kafka** & **Apache Spark** communities for excellent documentation
- **PySpark MLlib** for distributed machine learning capabilities
- **Flask** for lightweight web framework
- **Google Cloud Platform** for VM infrastructure

---

**Last Updated:** December 23, 2024
