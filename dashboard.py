import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st

from config import PROCESSED_CSV


st.set_page_config(
    page_title="Energy Stream",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ Energy Stream")
st.subheader("Real-Time Energy Utilization Analytics")

st.markdown(
    "IoT energy readings flow through Apache Kafka into an Apache Spark "
    "pipeline (cleaning, features, Isolation Forest) before this dashboard."
)

CSV_FILE = PROCESSED_CSV

if not os.path.exists(CSV_FILE):
    st.warning("Waiting for Spark pipeline output...")
    st.stop()

df = pd.read_csv(CSV_FILE)

if df.empty:
    st.warning("No energy data available yet.")
    st.stop()

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

for column in ["power", "energy", "temperature", "voltage", "current"]:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

if "status" not in df.columns:
    df["status"] = "NORMAL"
if "alert" not in df.columns:
    df["alert"] = ""
if "anomaly_score" not in df.columns:
    df["anomaly_score"] = 0.0
if "is_anomaly" not in df.columns:
    df["is_anomaly"] = df["status"].isin(["HIGH", "ANOMALY"])

latest_data = df.sort_values("timestamp").groupby("device_id").tail(1)

total_power = latest_data["power"].sum()
total_energy = df["energy"].sum()
active_devices = latest_data["device_id"].nunique()
active_alerts = latest_data["status"].isin(["HIGH", "ANOMALY"]).sum()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("⚡ Current Power", f"{total_power / 1000:.2f} kW")

with col2:
    st.metric("🔋 Energy Recorded", f"{total_energy:.4f} kWh")

with col3:
    st.metric("📱 Active Devices", active_devices)

with col4:
    st.metric("🚨 Active Alerts", int(active_alerts))

st.divider()

st.subheader("📈 Real-Time Power Consumption")

power_over_time = (
    df.groupby("timestamp")["power"]
    .sum()
    .reset_index()
    .sort_values("timestamp")
    .tail(30)
)
power_over_time["time"] = power_over_time["timestamp"].dt.strftime("%H:%M:%S")
power_over_time["power_kw"] = power_over_time["power"] / 1000

fig_power = px.line(
    power_over_time,
    x="time",
    y="power_kw",
    markers=True,
    labels={"time": "Time", "power_kw": "Power (kW)"},
    title="Real-Time Total Power Consumption (Recent)",
)
fig_power.update_layout(height=450)
st.plotly_chart(fig_power, use_container_width=True)

st.subheader("📊 Device-Wise Current Power")

device_cols = ["device", "location", "power", "status"]
if "anomaly_score" in latest_data.columns:
    device_cols.append("anomaly_score")

device_power = latest_data[device_cols].copy().sort_values("power", ascending=False)

fig_device = px.bar(
    device_power,
    x="device",
    y="power",
    color="status",
    hover_data=[c for c in device_cols if c not in ("device", "power")],
    labels={"power": "Power (W)", "device": "Device"},
    title="Current Power Consumption by Device",
    color_discrete_map={
        "NORMAL": "#2ca02c",
        "HIGH": "#d62728",
        "ANOMALY": "#ff7f0e",
    },
)
fig_device.update_layout(height=450)
st.plotly_chart(fig_device, use_container_width=True)

st.subheader("🚨 Energy Consumption Alerts")

alerts = latest_data[latest_data["status"].isin(["HIGH", "ANOMALY"])].copy()

if alerts.empty:
    st.success("✅ No abnormal energy consumption detected.")
else:
    st.error(
        f"⚠️ {len(alerts)} device(s) currently show high consumption "
        "or Isolation Forest anomalies."
    )
    alert_cols = [
        c
        for c in [
            "timestamp",
            "device",
            "location",
            "power",
            "status",
            "anomaly_score",
            "alert",
        ]
        if c in alerts.columns
    ]
    st.dataframe(alerts[alert_cols], use_container_width=True, hide_index=True)

st.subheader("📡 Latest IoT Sensor Readings")

display_cols = [
    c
    for c in [
        "timestamp",
        "device_id",
        "device",
        "location",
        "voltage",
        "current",
        "power",
        "temperature",
        "power_ratio",
        "anomaly_score",
        "status",
    ]
    if c in latest_data.columns
]

display_data = latest_data[display_cols].sort_values("power", ascending=False)
st.dataframe(display_data, use_container_width=True, hide_index=True)

st.divider()
st.caption(
    "Energy Stream | Kafka + Apache Spark + Isolation Forest + Streamlit"
)

time.sleep(2)
st.rerun()
