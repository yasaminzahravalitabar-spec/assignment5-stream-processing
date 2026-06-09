# Real-Time Stream Processing Assignment

**Student:** Yasaminzahra Valitabar  
**Student ID:** 101001242  
**Course:** ENGR 5785G: Real-time Data Analytics for IoT  
**Topic:** Real-Time Stream Processing  
**Scenario Chosen:** B - Hospital Patient Monitoring

## Project Overview

This project builds a Spark Structured Streaming pipeline for hospital patient monitoring. The goal is to detect patients whose heart rate stays high over time, instead of reacting to one single spike.

The pipeline reads patient heart-rate readings from a watched folder, simulating a real-time stream. It then calculates the average heart rate for each patient in 2-minute tumbling windows. If a patient has an average heart rate above 100 bpm in two consecutive windows, the job prints a clinical alert in the Spark console.

## Why I Chose This Window Type

I chose the 2-minute tumbling window because the alert is about sustained abnormal heart rate. In a hospital setting, one high reading may happen because of movement, stress, sensor noise, or a temporary spike. A tumbling window gives a clean fixed time block where Spark can summarize several readings together.

Using two consecutive tumbling windows makes the alert more reliable. It means the patient's average heart rate stayed above 100 bpm for more than one window, so the system is not only reacting to a single measurement. This matches the assignment scenario because the goal is to detect sustained elevated heart rate across ICU patient streams.

## Where the Pipeline Requires State

This pipeline requires state in three places:

1. Spark keeps state for the 2-minute window aggregation because it has to collect heart-rate readings by patient and event-time window before calculating the average.

2. The watermark keeps event-time state under control. The code uses `withWatermark("event_time", "4 minutes")`, so Spark can handle slightly late data without keeping old windows forever.

3. The alert logic keeps track of the previous elevated window for each patient. This is needed because the alert only fires when the current elevated window directly follows another elevated window for the same patient.

## Files in This Project

`hospital_patient_monitoring.py` is the main Spark Structured Streaming job.

`feed_stream.py` copies sample CSV files into the watched input folder one at a time to simulate streaming.

`data/source/` contains small sample CSV batches used for the demo.

`data/stream_input/` is the watched directory used by Spark `readStream`.

`checkpoints/` is used by Spark for streaming checkpoint data.

`requirements.txt` lists the Python dependency.

## Input Data Format

The stream expects CSV files with this format:

```csv
patient_id,event_time,heart_rate
P001,2026-06-08 10:00:05,88
P002,2026-06-08 10:00:10,104
```

The important columns are:

`patient_id`: unique patient identifier

`event_time`: timestamp for the reading

`heart_rate`: heart rate value in beats per minute

## How to Run

These commands should be run from the project folder:

```powershell
cd "C:\Users\yasam\OneDrive\Desktop\assignment5-stream-processing"
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install PySpark:

```powershell
pip install -r requirements.txt
```

Start the Spark streaming job in the first terminal:

```powershell
python hospital_patient_monitoring.py
```

Open a second terminal, activate the same environment, and feed the sample files into the stream:

```powershell
cd "C:\Users\yasam\OneDrive\Desktop\assignment5-stream-processing"
.\.venv\Scripts\Activate.ps1
python feed_stream.py
```

The streaming job watches `data/stream_input/`. The helper script copies one CSV batch at a time into that folder, which simulates new patient monitor data arriving.

## Alert Condition

The alert condition is:

```text
Average heart rate > 100 bpm in two consecutive 2-minute windows for the same patient
```

When this happens, the console prints a message like:

```text
CLINICAL ALERT | Patient P002 had sustained elevated heart rate across two consecutive 2-minute windows. Current window: 2026-06-08 10:02:00 to 2026-06-08 10:04:00, average HR: 109.0 bpm
```

With the included sample data, patient `P002` should trigger an alert after the second batch because that patient has an average heart rate over 100 bpm in the first two consecutive windows. Patient `P001` should trigger later because that patient becomes elevated in two later consecutive windows.

## Screenshot to Include

After running the project, take a screenshot of the first terminal when the alert appears. The screenshot should show the Spark console output containing a line like:

```text
CLINICAL ALERT | Patient P002 had sustained elevated heart rate across two consecutive 2-minute windows.
```

That screenshot satisfies the assignment requirement for alert output firing in the Spark console.

## Notes

The assignment mentions the IoMT Health Monitoring dataset from Kaggle. Kaggle downloads usually require a Kaggle account, so this project includes a small sample dataset with the same type of fields needed for the pipeline. If the real Kaggle file is downloaded, it can be used by converting or renaming its relevant columns to `patient_id`, `event_time`, and `heart_rate`, then placing CSV files into `data/stream_input/`.

The main technical requirements are covered:

`readStream` is used with a watched directory.

`withWatermark` is applied to event time.

A 2-minute tumbling window aggregation is used.

A filtered alert stream is created for elevated heart-rate windows.

The console output fires a clinical alert when the same patient is elevated in two consecutive windows.
