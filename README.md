# ⚡ HW-PiCollector

*Open-source Raspberry Pi-based smart energy data logger and sync system using HomeWizard and Cassandra.*

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](./LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Raspberry%20Pi-lightgrey.svg)](https://www.raspberrypi.com/)
[![Database](https://img.shields.io/badge/Database-Cassandra-blue.svg)](https://cassandra.apache.org/)

A lightweight and modular data collection pipeline for residential electricity **consumption and production monitoring** using **HomeWizard** sensors and a **Raspberry Pi**. Designed to collect, store, and sync data with a central Cassandra database for smart metering and microgrid analysis.

---

## 🎯 Objective
- Collect real-time (1-second interval) energy data from distributed households.
- Perform asynchronous synchronization to a centralized Cassandra database.
- Remotely monitor Raspberry Pis and connected sensors to ensure operational reliability.
- Preserve data privacy and security through a dedicated ULB VPN tunnel.

---

## 📦 Features

- 🏠 Real-time energy monitoring for both production and consumption using HomeWizard (P1 and kWh 1/3-phase meters).
- 🍓 Lightweight deployment on Raspberry Pi (Linux-based).
- 💾 Local buffering of data in CSV format before synchronization.
- 🗄️ Scalable, high-throughput ingestion into a Cassandra time series database.
- 📡 Scheduled synchronization from edge devices to the central database.
- 🔒 Secure credential handling and encrypted data transport over VPN.
- ⚙️ Automated provisioning, configuration, and updates via Ansible.
- 🔄 Resilient systemd service for always-on data collection.
- 🔧 Explore the [project structure](#project-structure) for detailed file descriptions.

---

## 🔌 Component Overview

### 🖥️ System Services

- **`coomep_data_collection.service`**
- systemd service that launches the data collection script on RPi boot
- Depends on VPN and network availability
- Ensures persistent and automatic restart of the service

### 🛠️ Ansible Deployment Scripts

- **`deploy_env.yml`**: Sets up required folders on the Raspberry Pi (e.g. `/opt/coomep`)
- **`deploy_files.yml`**: Copies all necessary scripts, credentials, and configs to RPi
- **`deploy_packages.yml`**: Installs required system packages and Python dependencies
- **`activate_services.yml`**: Enables and starts the systemd service on the RPi
- **`monitor_sensors.yml`**: Monitors disk usage, memory, CPU, and HomeWizard connectivity

### 🔐 Secure VPN Connection

- **`vpn_config.yml`** (not included): This config connects the RPi to ULB's secure VPN
- Ensures secure, encrypted data transmission
- Enables access to the remote Cassandra database
- Required to run the service successfully

---

## 💼 Dependencies

### 🐍 Python Version

This project is developed and tested with **Python 3.12**.

### 🍓 Raspberry Pi Environment

All Python dependencies required for the Raspberry Pi are specified in [`requirements.txt`](./requirements.txt).  
These include packages needed for real-time data collection, local CSV buffering, and communication with HomeWizard sensors.

> ✅ These dependencies are automatically installed using the `deploy_packages.yml` Ansible playbook.  
> No manual installation is required on the Raspberry Pi.

> 🛠️ The full Python environment setup (including system packages and virtualenv creation)  
> is handled by the `deploy_packages.yml` script for consistent and reproducible deployments.

### 🗄️ Backend (Cassandra Synchronization)

The following additional packages are required on the backend server to enable synchronization with Cassandra:

```txt
pandas
cassandra-driver
bcrypt
```

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

## 🔒 Security Notice

- Private files such as credentials.json, vpn_config.yml, or user passwords must not be committed to the repository.
- Ensure that your ansible and systemd scripts reference relative paths or environment variables instead of personal file paths.

---

## 📁 Project Structure

```bash
HW-PiCollector/
├── credentials.json                    # Cassandra credentials (not versioned)
├── users_config.xlsx                   # Config for all users and installations
├── requirements.txt                    # Python dependencies for Raspberry Pi
├── systemd/
│   └── coomep_data_collection.service  # systemd service for automatic data collection
├── ansible/
│   ├── activate_services.yml           # Enable & start the systemd service
│   ├── deploy_env.yml                  # Set up folders and initial environment
│   ├── deploy_files.yml                # Copy code/config to RPi
│   ├── deploy_packages.yml             # Install system packages & Python deps
│   ├── monitor_sensors.yml             # Monitor sensor health and system metrics
│   └── vpn_config.yml                  # ULB VPN config (not included for security)
└── src/
    ├── collect_homewizard_data.py      # Main script to collect sensor data (runs on RPi)
    ├── config.py                       # Shared constants (paths, DB info)
    ├── py_to_cassandra.py              # Functions to interact with Cassandra
    ├── sync_homewizard.py              # Sync RPi data to Cassandra
    └── utils.py                        # Helper utilities (logging, parsing, etc.)
```

---

## 📃 License

MIT License — see [LICENSE](./LICENSE)

---

## 🤝 Acknowledgments

This project is developed within the context of academic collaboration with Université Libre de Bruxelles (ULB).

---

## 📧 Contact

👤 Brice Petit

📧 brice\[dot\]petit\[at\]ulb\[dot\]be

📍 IRIDIA, ULB — Brussels, Belgium
