from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DoubleType


BASE_DIR = Path(__file__).resolve().parent
STREAM_INPUT_DIR = str(BASE_DIR / "data" / "stream_input")
CHECKPOINT_DIR = str(BASE_DIR / "checkpoints" / "hospital_patient_monitoring")


def main():
    spark = (
        SparkSession.builder
        .appName("Hospital Patient Monitoring - Sustained Heart Rate Alerts")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("patient_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("heart_rate", DoubleType(), False),
        ]
    )

    readings = (
        spark.readStream
        .schema(schema)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(STREAM_INPUT_DIR)
    )

    windowed_average = (
        readings
        .withWatermark("event_time", "4 minutes")
        .groupBy(
            F.window("event_time", "2 minutes").alias("window"),
            F.col("patient_id"),
        )
        .agg(F.avg("heart_rate").alias("avg_heart_rate"))
    )

    elevated_windows = (
        windowed_average
        .filter(F.col("avg_heart_rate") > 100)
        .select(
            F.col("patient_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.round("avg_heart_rate", 2).alias("avg_heart_rate"),
        )
    )

    previous_elevated_windows = {}

    def print_alerts(batch_df, batch_id):
        nonlocal previous_elevated_windows

        rows = batch_df.orderBy("patient_id", "window_start").collect()
        if not rows:
            return

        print(f"\n========== Batch {batch_id}: Sustained Heart Rate Check ==========")

        current_elevated_windows = {}
        alert_count = 0

        for row in rows:
            patient_id = row["patient_id"]
            window_start = row["window_start"]
            window_end = row["window_end"]
            avg_heart_rate = row["avg_heart_rate"]

            current_elevated_windows[patient_id] = window_end
            previous_window_end = previous_elevated_windows.get(patient_id)

            if previous_window_end == window_start:
                alert_count += 1
                print(
                    "CLINICAL ALERT | "
                    f"Patient {patient_id} had sustained elevated heart rate "
                    f"across two consecutive 2-minute windows. "
                    f"Current window: {window_start} to {window_end}, "
                    f"average HR: {avg_heart_rate} bpm"
                )

        if alert_count == 0:
            print("No sustained clinical alerts in this batch.")

        previous_elevated_windows = current_elevated_windows

    query = (
        elevated_windows.writeStream
        .outputMode("update")
        .foreachBatch(print_alerts)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming job started.")
    print(f"Watching folder: {STREAM_INPUT_DIR}")
    print("Copy CSV files into that folder to simulate the patient monitor stream.")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
