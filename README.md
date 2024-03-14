# Snowflake connector connection and Unified API Demo

## Snowflake Python Integration Showcase

Explore the versatility of Snowflake with Python through two distinct methods: the traditional Snowflake Connector and the newer unified Snowflake API, which includes Snowpark sessions. This repository provides examples demonstrating various operations such as DDL (Data Definition Language), DML (Data Manipulation Language), and resource management using both approaches.

## Getting Started

This guide assumes you have a basic understanding of Python and SQL. Before running the examples, set up a Python virtual environment and install the necessary Snowflake package.

### Prerequisites

- Python 3.x installed on your system.
- Access to a Snowflake account.

### Setting Up Your Environment

1. Create and activate a Python virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

2. Install the Snowflake Python package:
   ```bash
    pip install snowflake -U

This package includes everything needed for both the Snowflake Connector and Snowpark API integrations.

Examples Overview

Connector Example: Demonstrates how to use the Snowflake Connector to execute SQL statements, manage Snowflake resources, and perform data operations.
Unified API Library Example: Shows how to establish a session with Snowflake using the Snowpark API and perform data frame operations, akin to working with Pandas but on Snowflake's powerful computing platform.
Each script is annotated to guide you through the operations being performed, from connecting to Snowflake to executing SQL commands and managing data.

Usage

Replace placeholder values in the provided scripts with your actual Snowflake account details, then run the scripts to perform operations in Snowflake. Ensure best practices for secure credential management.

Additional Resources

For more detailed information on using Snowflake with Python, refer to the official documentation:

Snowflake Python API Overview

Contributing

Feedback and contributions are welcome! Please feel free to fork the repository, make changes, and submit pull requests. If you encounter any issues or have suggestions for improvement, please file an issue.
