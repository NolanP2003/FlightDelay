# FlightDelay

### How to Use Streaming Service
There are five files in the Main Folder named Step1_Data_Visualization.ipynb, Step2_Batch_Processing.ipynb, Step3_Streaming_Prediction.ipynb, app.py, and Kafka_Producer.py.

There is one file relevant in the main folder called Step1_Data_Visualization.ipynb that is where all the visualizations are done.

Put these files into a folder (I named my folder "Project") on your VM on GCP. In this folder, you should have these five files plus the dataset titled "flight_data.csv". First, make sure Kafka is running by opening a terminal and typing "sudo systemctl status kafka". You should run this command in the terminal on the home directory (cd ../). It should say Kafka is active. Then, go into your Jupyter Lab and run all the lines of code in the Step2 file. This will save a gbt pipeline and gbt model to your project folder which will be used in the next steps. In another terminal, cd into the project folder and run the Kafka Producer. To do this, either type python Kafka_Producer.py (if you have a different version use python3 Kafka_Producer.py). This will start the streaming using Kafka - keep this terminal running throughout the streaming process. Just leave it running in the background. Once that is going, go back to Jupyter Lab and run the Step3 file. It will begin streaming the data and should work, showing you the batched and predictions as they come in. 

To run our application open up another terminal (cd into the project folder if not already in there). To do this, either type python app.py (if you have a different version use python3 app.py). This will start the streaming on our website - keep this terminal running to host the website and all. 

### Dataset Application

There are two folders in the Main Folder called Dataset Application and Visualizations. Put these folders into a different folder on your personal computer. Open this new folder in VSCode and make sure you have an extension installed called "LiveServer" by Ritwick Dey. 

In the explorer, expand your Dataset Application folder and then double click onto the index.html and click the "Open with Live Server" option. This should automatically open a tab of your preferred browser that is a locally hosted version of the dataset application. If it doesn't open go automatically to the page, enter into the url address bar: http://127.0.0.1:5500/Dataset%20Application/index.html