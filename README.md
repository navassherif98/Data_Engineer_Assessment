## Aidetic Data Engineer Assignment

### PySpark Earthquake Analysis

**Overview :** This PySpark application performs data analysis on earthquake data. It covers tasks such as loading data, data transformation, and geographical visualization.

**Prerequisites :** PySpark 3.5 or later , Java and Hadoop dependencies

**Apache Spark Installation links :**
1. [Download JDK](https://www.oracle.com/in/java/technologies/downloads/#jdk19-windows)
2. [Download Python](https://www.python.org/downloads/)
3. [Download Spark](https://spark.apache.org/downloads.html)
4. [Download Winutils Hadoop-3.3.5/bin/winutils.exe](https://github.com/cdarlint/winutils)
5. Setup Environment Variables:-
    - HADOOP_HOME = C:\hadoop
    - JAVA_HOME = C:\java\jdk
    - SPARK_HOME = C:\spark\spark-3.3.1-bin-hadoop2
6. Setup Required Paths:
    - %SPARK_HOME%\bin
    - %HADOOP_HOME%\bin
    - %JAVA_HOME%\bin

**Instructions :**
1. Set up the Environment
    Ensure PySpark 3.0 or later is installed.
    Follow the installation links to download and set up JDK, Python, and Apache Spark. Additionally, configure environment variables and required paths.
2. Activate Virtual Environment
    Activate virtual environment using *source .venv/bin/activate*
3. Access and Save Dataset
    Download the dataset from Data/database.csv.
    Save it in the data directory.
4. Run the PySpark Application
    Execute the run.py script in the src/main/python directory using your virtual environment.
5. Output
    The final result will be saved in the output directory.
   
**Tasks Completed :**
- [x] 1. Loaded the dataset into a PySpark DataFrame.
- [x] 2. Converted the Date and Time columns into a Timestamp column.
- [x] 3. Filtered earthquakes with a magnitude greater than 5.0.
- [x] 4. Calculated the average depth and magnitude for each earthquake type.
- [x] 5. Implemented a UDF to categorize earthquakes into levels based on magnitudes.
- [x] 6. Calculated the distance of each earthquake from a reference location.
- [x] 7. Visualized the geographical distribution of earthquakes.

### THANK YOU
