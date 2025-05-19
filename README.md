# ⚡ HW-PiCollector

A lightweight and modular data collection pipeline for residential electricity **consumption and production monitoring** using **HomeWizard** sensors and a **Raspberry Pi**. Designed to collect, store, and sync data with a central Cassandra database for smart metering and microgrid analysis.

---

## 📦 Features

- 🏠 Collects real-time power data from HomeWizard (P1 and kWh 1/3-phase meters)
- 🍓 Runs on Raspberry Pi (Linux-based systems)
- 💾 Local storage of collected data (CSV)
- 📡 Periodic synchronization to a **remote Cassandra database**
- 🔒 Secure credential management
- ⚙️ Deployable via **Ansible**
- 📈 Monitoring tools for data freshness and system uptime

---

## 📁 Project Structure

```bash
HW-PiCollector/
├── src/
│   ├── collect_homewizard_data.py   # Collects data directly from HomeWizard devices on the RPi
│   ├── config.py                    # Handles configuration for Cassandra syncing
│   ├── py_to_cassandra.py          # Uploads data from CSV to Cassandra DB
│   ├── sync_homewizard.py          # Syncs RPi-collected data with the remote DB
│   ├── utils.py                    # Helper functions for parsing, formatting, etc.
│
├── systemd/                         # Service files to enable data collection on boot
│   └── homewizard-collector.service
│
├── credentials.json                 # (Not tracked) Auth credentials for the remote Cassandra DB
├── users_config.xlsx                # Describes the sensors/sites used
├── requirements.txt                 # Python dependencies for Raspberry Pi
├── README.md
├── LICENSE
```

---

## 🔍 Component Overview

| Component                   | Description                                                                                       |
|-----------------------------|---------------------------------------------------------------------------------------------------|
| `collect_homewizard_data.py`| Collects data from HomeWizard (P1 / kWh meter) on the Raspberry Pi                                |
| `py_to_cassandra.py`        | A full-featured Cassandra I/O module to manage database interactions                              |
| `sync_homewizard.py`        | Module for synchronizing HomeWizard sensor data from Raspberry Pi devices to a Cassandra database |
| `config.py`                 | Centralized configuration for DB connection and paths                                             |
| `utils.py`                  | Utility functions used across the pipeline                                                        |
| `requirements.txt`          | Packages to install on the Raspberry Pi                                                           |
| `systemd/*.service`         | Used to run the collection service at boot time                                                   |

---

## ☁️ Deployment Context

- Runs on **Raspberry Pi** devices connected to **HomeWizard** sensors (P1, kWh meter)
- Collected data is stored **locally**, then synchronized to a **central Cassandra** database
- Systemd services and Ansible automation help maintain uptime and deployment consistency

---

## 💡 Applications

- Smart metering of households
- Solar self-consumption analysis
- Microgrid monitoring and simulation
- Academic and applied research in energy forecasting

---

## 📃 License

MIT License — see [LICENSE](./LICENSE)

---

## 🤝 Acknowledgments

This project is developed within the context of academic collaboration with Université Libre de Bruxelles (ULB).

---

## 🧠 Contact

👤 Brice Petit

📧 brice\[dot\]petit\[at\]ulb\[dot\]be

📍 IRIDIA, ULB — Brussels, Belgium
