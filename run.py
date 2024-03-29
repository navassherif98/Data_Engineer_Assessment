from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Your PySpark code here
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp, avg
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("Aidetic").master("local[*]").getOrCreate()

    print("Application Started")
    print("Spark Version : ",spark.version)

    print("\nStep 1: Load the dataset into a PySpark DataFrame")
    folder_path = "C:/Users/Navas/PycharmProjects/Data_Engineer_Assessment/"
    input_path = folder_path + "Data/database.csv"
    output_path = folder_path + "Output"
    map_output_path = folder_path + "Map_Output/worldmap.html"

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.select("Date", "Time", "Latitude", "Longitude", "Type", "Depth", "Magnitude")

    print("Total count : ", df.count())
    print("Sample records :- ")
    df.show(5)
    df.printSchema()
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        print(f"Column '{col_name}' has {null_count} NULL values.")

    print("\nStep 2: Convert the Date and Time columns into a timestamp column named Timestamp")
    df = df.withColumn("only_time", F.date_format(df["time"], "HH:mm:ss"))
    df.show(3)
    print("Dataframe with Timestamp")
    df_timestamp = df.withColumn("Timestamp", to_timestamp(F.concat_ws(" ", col("Date"), col("only_time")),                                                           "MM/dd/yyyy HH:mm:ss")).drop("only_time")
    df_timestamp.show(3)

    print("\nStep 3: Filter the dataset to include only earthquakes with a magnitude greater than 5.0")
    df_filtered = df.filter(col("Magnitude") > 5.0)

    print("\nStep 4: Calculate the average depth and magnitude of earthquakes for each earthquake type")
    df_avg = df_filtered.groupBy("Type").agg(avg("Depth").alias("AvgDepth"), avg("Magnitude").alias("AvgMagnitude"))
    df_avg.show()


    print("\nStep 5: Implement a UDF to categorize the earthquakes into levels")
    def categorize_magnitude(magnitude):
        if magnitude < 6.0:
            return "Low"
        elif 6.0 <= magnitude < 8.0:
            return "Moderate"
        else:
            return "High"

    categorize_magnitude_udf = udf(categorize_magnitude, StringType())
    df_categorized = df_filtered.withColumn("MagnitudeLevel", categorize_magnitude_udf(col("Magnitude")))
    df_categorized.describe()

    print("\nStep 6: Calculate the distance of each earthquake from a reference location")
    df_distance = df_filtered.withColumn("DistanceFromReference",
                                         F.sqrt((col("Latitude") - 0) ** 2 + (col("Longitude") - 0) ** 2))

    print("\nStep 7: Visualize the geographical distribution of earthquakes")
    import folium
    from folium.plugins import MarkerCluster

    # Create a Folium map centered at a specific location (e.g., world center)
    earthquake_map = folium.Map(location=[0, 0], zoom_start=2)

    # Create a MarkerCluster to group earthquake markers for better visualization
    marker_cluster = MarkerCluster().add_to(earthquake_map)

    # Add markers for each earthquake to the map
    for row in df.collect():
        folium.Marker(
            location=[row["Latitude"], row["Longitude"]],
            popup=f"Magnitude: {row['Magnitude']}, Depth: {row['Depth']}",
        ).add_to(marker_cluster)

    earthquake_map.save(map_output_path)
    print("worldmap saved Successfully")


    print("\nStep 8: Save the final CSV")
    df_filtered.coalesce(1).write.mode("overwrite").option("header", "true").format("csv").save(output_path)
    print("Final result is loaded into the path :",output_path)

    # Stop the Spark session
    spark.stop()
    print("Application Successfully Completed")