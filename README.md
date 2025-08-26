# HW-PiCollector 🌍⚡

![HW-PiCollector](https://img.shields.io/badge/version-1.0.0-blue.svg) ![License](https://img.shields.io/badge/license-MIT-green.svg) ![Stars](https://img.shields.io/github/stars/PRATIK123213/HW-PiCollector.svg) ![Forks](https://img.shields.io/github/forks/PRATIK123213/HW-PiCollector.svg)

HW-PiCollector is a lightweight and modular data collection system for residential energy monitoring. It leverages HomeWizard devices (P1 and kWh 1/3-phase meters) connected to a Raspberry Pi, collecting electricity consumption and production data in real-time.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Storage](#data-storage)
- [Monitoring and Visualization](#monitoring-and-visualization)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Releases](#releases)

## Features

- **Real-Time Data Collection**: Collects electricity consumption and production data instantly.
- **Modular Design**: Easily extendable to include more sensors or devices.
- **HomeWizard Compatibility**: Works seamlessly with HomeWizard P1 and kWh meters.
- **Raspberry Pi Integration**: Designed to run on Raspberry Pi, making it affordable and accessible.
- **Data Visualization**: Offers tools to visualize your energy usage and production.

## Installation

To install HW-PiCollector, follow these steps:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/PRATIK123213/HW-PiCollector.git
   cd HW-PiCollector
   ```

2. **Install Dependencies**:

   Use Ansible to set up the required dependencies.

   ```bash
   ansible-playbook setup.yml
   ```

3. **Configure Your Devices**:

   Ensure your HomeWizard devices are connected and configured properly.

## Usage

To start collecting data, run the following command:

```bash
python main.py
```

This will initiate the data collection process. You can check the logs for any errors or issues.

## Configuration

The configuration file is located in the `config` directory. You can edit `config.yaml` to set your device parameters and data storage options.

### Example Configuration

```yaml
homewizard:
  device_id: "YOUR_DEVICE_ID"
  api_key: "YOUR_API_KEY"

storage:
  type: "cassandra"
  host: "localhost"
  port: 9042
```

## Data Storage

HW-PiCollector supports various data storage options. By default, it uses Cassandra for time-series data. You can configure this in the `config.yaml` file. 

### Setting Up Cassandra

To set up Cassandra, follow these steps:

1. **Install Cassandra**:

   ```bash
   sudo apt-get install cassandra
   ```

2. **Start Cassandra**:

   ```bash
   sudo service cassandra start
   ```

3. **Create Keyspace**:

   Connect to Cassandra and create a keyspace for storing data.

   ```sql
   CREATE KEYSPACE energy_data WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
   ```

## Monitoring and Visualization

To visualize your energy data, you can use Grafana or any other visualization tool compatible with your data storage solution.

### Setting Up Grafana

1. **Install Grafana**:

   ```bash
   sudo apt-get install grafana
   ```

2. **Start Grafana**:

   ```bash
   sudo service grafana-server start
   ```

3. **Access Grafana**:

   Open your browser and go to `http://localhost:3000`. Use the default login credentials to access the dashboard.

4. **Add Data Source**:

   Configure Grafana to connect to your Cassandra database.

## Contributing

We welcome contributions! If you want to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and commit them.
4. Push to your fork and create a pull request.

## License

HW-PiCollector is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or issues, please reach out to the maintainer:

- **Name**: Pratik
- **Email**: pratik@example.com

## Releases

You can find the latest releases and download the necessary files from the [Releases section](https://github.com/PRATIK123213/HW-PiCollector/releases). Make sure to download and execute the files as needed.

## Conclusion

HW-PiCollector offers a robust solution for monitoring energy usage in residential settings. With its modular design and real-time data collection, it empowers users to make informed decisions about their energy consumption. We encourage you to explore the features and contribute to the project. Thank you for your interest!