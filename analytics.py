from config import HIGH_POWER_MULTIPLIER


def calculate_power_status(power, base_power):
    if power > base_power * HIGH_POWER_MULTIPLIER:
        return "HIGH"
    return "NORMAL"


def calculate_energy_kwh(power, seconds=2):
    hours = seconds / 3600
    energy = (power * hours) / 1000
    return round(energy, 8)


def get_alert_message(status, device, power):
    if status == "HIGH":
        return f"High energy consumption detected: {device} ({power} W)"
    if status == "ANOMALY":
        return f"Isolation Forest anomaly: {device} ({power} W)"
    return ""
