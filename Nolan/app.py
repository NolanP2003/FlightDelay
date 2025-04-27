from flask import Flask, render_template_string
import pandas as pd
import glob
import os

app = Flask(__name__)

STREAMING_OUTPUT_PATH = "./streaming_predictions_output/"

html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Flight Delay Predictions</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }
        h1 { color: #0056b3; text-align: center; margin-bottom: 30px; }
        table { border-collapse: collapse; width: 100%; box-shadow: 0 2px 3px rgba(0,0,0,0.1); background-color: #fff; }
        th, td { text-align: left; padding: 12px 15px; border-bottom: 1px solid #ddd; }
        th { background-color: #007bff; color: white; text-transform: uppercase; font-size: 0.85em; letter-spacing: 0.05em;}
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #f1f1f1; }
        td.delay-predicted { color: #dc3545; font-weight: bold; }
        td.no-delay { color: #28a745; }
        .probability { font-style: italic; color: #555; }
        .no-data { text-align: center; color: #777; margin-top: 50px; font-size: 1.2em; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #888; }
    </style>
</head>
<body>
    <h1>Real-Time Flight Delay Predictions</h1>
    {% if flights %}
    <table>
        <thead>
            <tr>
                <th>Flight Date</th>
                <th>Airline</th>
                <th>Origin</th>
                <th>Destination</th>
                <th>Sched. Dep. (HHMM)</th>
                <th>Prediction</th>
                <th>Probability Severe Delay (%)</th>
            </tr>
        </thead>
        <tbody>
            {% for doc in flights %}
            <tr>
                <td>{{ doc.get('FL_DATE', '-') }}</td>
                <td>{{ doc.get('AIRLINE_CODE', '-') }}</td>
                <td>{{ doc.get('ORIGIN', '-') }}</td>
                <td>{{ doc.get('DEST', '-') }}</td>
                <td>{{ "{:04d}".format(doc.get('CRS_DEP_TIME', 0)) if doc.get('CRS_DEP_TIME') is not none else '-' }}</td>
                <td class="{{ 'delay-predicted' if doc.get('Prediction_Label', '').startswith('Severe') else 'no-delay' }}">
                    {{ doc.get('Prediction_Label', '-') }}
                </td>
                <td class="probability">
                    {{ "{:.2f}".format(doc.get('Probability_Severe_Delay', 0) * 100) }}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
    {% else %}
        <p class="no-data">No flight predictions available yet. Waiting for data...</p>
    {% endif %}
    <div class="footer">Page refreshes automatically every 10 seconds.</div>
</body>
</html>
"""

def load_predictions_from_files():
    try:
        parquet_files = glob.glob(os.path.join(STREAMING_OUTPUT_PATH, "*.parquet"))
        if not parquet_files:
            print("No parquet files yet.")
            return []
        
        df = pd.concat([pd.read_parquet(f) for f in parquet_files])

        df = df.sort_values(by=["FL_DATE", "CRS_DEP_TIME"], ascending=[False, False])

        flights = df.to_dict(orient="records")

        return flights[:50]
    except Exception as e:
        print(f"Error loading streaming data: {e}")
        return []

@app.route('/')
def index():
    flights = load_predictions_from_files()
    return render_template_string(html_template, flights=flights)

if __name__ == '__main__':
    print("? Flask app started!")
    app.run(host="0.0.0.0", port=5000, debug=True)
