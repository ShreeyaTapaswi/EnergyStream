import random
from datetime import datetime


# Virtual IoT devices
DEVICES = [
    {
        "device_id": "DEV_001",
        "device": "Air Conditioner",
        "location": "Bedroom",
        "base_power": 1400
    },
    {
        "device_id": "DEV_002",
        "device": "Refrigerator",
        "location": "Kitchen",
        "base_power": 250
    },
    {
        "device_id": "DEV_003",
        "device": "Television",
        "location": "Living Room",
        "base_power": 120
    },
    {
        "device_id": "DEV_004",
        "device": "Washing Machine",
        "location": "Utility Room",
        "base_power": 900
    },
    {
        "device_id": "DEV_005",
        "device": "Computer",
        "location": "Study Room",
        "base_power": 350
    },
    {
        "device_id": "DEV_006",
        "device": "Ceiling Fan",
        "location": "Bedroom",
        "base_power": 80
    },
    {
        "device_id": "DEV_007",
        "device": "Microwave",
        "location": "Kitchen",
        "base_power": 1100
    },
    {
        "device_id": "DEV_008",
        "device": "Lights",
        "location": "Hall",
        "base_power": 100
    },
    {
        "device_id": "DEV_009",
        "device": "Water Heater",
        "location": "Bathroom",
        "base_power": 1800
    },
    {
        "device_id": "DEV_010",
        "device": "WiFi Router",
        "location": "Study Room",
        "base_power": 20
    }
]


def generate_reading(device):

    # Simulated voltage
    voltage = round(random.uniform(220, 240), 2)

    # Normal variation in device power
    variation = random.uniform(0.85, 1.15)

    power = device["base_power"] * variation

    # Occasionally simulate abnormal consumption
    if random.random() < 0.05:
        power *= random.uniform(2, 3)

    power = round(power, 2)

    # Calculate current using P = V × I
    current = round(power / voltage, 2)

    # Simulated temperature
    temperature = round(random.uniform(22, 35), 2)

    # Energy consumed during approximately one second
    energy = round(power / 3600000, 8)

    reading = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "device_id": device["device_id"],
        "device": device["device"],
        "location": device["location"],
        "voltage": voltage,
        "current": current,
        "power": power,
        "energy": energy,
        "temperature": temperature,
    }

    return reading
