# OmniHelper

## What's New

- \[2025.02.04\]: Added operator identification capability and fixed issues with function expression names and type matching.
- \[2025.01.15\]: Released OmniOperator 1.0.0 officially, which supports efficient analysis of expressions and function usage in execution logs.

## Project Introduction

### Overview

OmniHelper is a log analysis tool designed specifically for big data platforms, aimed at helping developers and operations personnel efficiently analyze the usage of operators, expressions, and functions in execution logs. As a supporting component of the Omni ecosystem, this tool can identify mixed execution of native operators and Omni operators, analyze related parameter types, detect unsupported operators/expressions/functions, and generate structured analysis reports to provide data support for performance optimization.

### Architecture

The core architectural components include:

- Command-line interface layer: Builds command-line parameter parsing using a parsing module, supports parameters such as input paths, output paths, and Java configurations, and provides user-friendly help information and usage examples.
- Log parsing module: Handles log file processing, supports automatic identification of log file patterns using regular expressions, and allows processing of single or multiple log files.
- Operator/Function/Expression analysis module: Efficiently analyzes the usage of operators, expressions, and functions in execution logs, identifies unsupported parts, and extracts operator execution time and resource consumption.
- Result processing module: Merges analysis results from multiple tasks, computes statistical metrics, and generates styled Excel reports.

### Application Scenarios

OmniHelper is primarily used for log analysis in big data platforms. It efficiently analyzes the usage of operators, expressions, and functions in execution logs, identifies unsupported operators, expressions, and functions, and generates structured analysis reports to provide data support for performance optimization.

### Concepts

Omni operators: High-performance operators, which use native code (C/C++) to replace physical operators at the bottom layer of big data, increasing the computing speed.

## Constraints

### Common constraints

To effectively plan and utilize the OmniHelper tool, it is recommended to be aware of potential risks and limitations.

- Trustlist scope limitation: The current analysis determines the support status of operators, functions, and expressions based on the trustlist in the `resources` folder. To extend the analysis scope, the trustlist must be expanded accordingly.
- Log file requirements: The current analysis is based on big data log files. The collected logs must contain operator information, and the log files must not be truncated.

## Directory Structure

The full project directory structure is as follows:

```bash





├── omnihelper/                                             # Project home directory
│   ├── docs/                                               # Document directory
│   │   ├── release_notes.md                                # OmniHelper Release Notes
│   ├── enum/                                               # Enumeration type definition directory
│   │   ├── type_enum.py                                    # Data enumeration
│   │   └── function_enum.py                                # Function enumeration
│   ├── util/                                               # Tool directory
│   │   ├── excel_util.py                                   # Excel processing utility
│   │   ├── common_util.py                                  # Common utility functions
│   │   └── func_util.py                                    # Function processing utility
│   ├── parser/                                             # Log parsing module directory
│   │   ├── op_parser.py                                    # Operator parser
│   │   ├── function_parser.py                              # Function parser
│   │   ├── type_matcher.py                                 # Type matcher
│   │   └── function_checker.py                             # Function checker
│   ├── resources/                                          # Resource file directory
│   │   ├── udf_dictionary.json                             # UDF dictionary
│   │   ├── omni_op_dictionary.json                         # Omni operator dictionary
│   │   ├── omni_function_dictionary.json                   # Omni function dictionary
│   │   └── omni_opname_mapping_dictionary.json             # Operator name mapping dictionary
│   ├── main.py                                             # Main program entry
│   ├── build.sh                                            # Build script
│   ├── README.md                                           # Project description document
│   └── __init__.py                                         # Python package initialization file
```

## Release Notes

For details about feature changes in each version, see [Release Notes](docs/en/release_notes.md).

## Environment Deployment

1. Installing the Java environment.
     JDK 1.8 is recommended. Ensure that the `java` command is available or specify the complete Java execution path in the parameter setting.

2. Prepare dependency JAR packages.

     - Method 1: Obtain the precompiled package `boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar` from the `resources` directory.

     - Method 2: If the precompiled package in the `resources` directory is not used, perform the following steps to build the package.
         1. Check that the build environment is ready.<br>Check environment: JDK 1.8, Maven 3.6 or later, Linux, macOS, or Windows (Git Bash/WSL is recommended for script execution.)

         2. Go to the build directory.

             ```bash
             cd omnihelper/omnimv-spark-extension
             ```

         3. Run the build script.

             ```bash
             bash build.sh
             ```

             After the build is complete, the generated JAR package is automatically copied to `omnihelper/resources/`.<br>
             The following is an example of the generated file:

              ```bash
              boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar

             ```

         4. Download Spark dependencies.<br>Obtain and extract [spark-3.4.3-bin-hadoop3.tgz](https://repo.huaweicloud.com/apache/spark/spark-3.4.3/). The extracted `jars` folder contains Spark dependencies.

3. Prepare event logs.
     - A single file or directory can be inputted. Note that you need to configure the output operator information through parameters and do not truncate logs.
     - The log file can be in `.lz4`, `.zstd`, or plain text format.

4. Prepare the Spark table structure information.<br>To improve type identification accuracy, you need to export the Spark table structure to `resources/spark_table_schema.csv`. The file must contain three columns: `full_table_name` (database_name. table_name), `column_name`, and `data_type`. The table name is in the format of the database name followed by a period (.) and the table name. A type containing commas (,) must be enclosed in double quotation marks ("").

     The following is an example of exporting the table structure:

     ```csv
     full_table_name,column_name,data_type
     test_db.table1,column1,bigint
     test_db.table1,column2,double
     test_db.table1,column3,"map<string,string>"
     test_db3.table2,column1,"decimal(20,4)"
     ```

     For details about how to export the Spark table structure, see the following PySpark-based script method:<br>
     1. Install PySpark.

         ```bash
         pip3 install pyspark==3.4.3
         ```

         Replace `3.4.3` with the actual Spark version.
     2. Run the script.

            ```python
            import csv
            import os
            from pyspark.sql import SparkSession

            def export_spark_schema_to_csv(output_path):
                """
                Export the table structures of all Spark databases to a local CSV file.
                """
                print("Initializing the SparkSession...")
                # Initialize the SparkSession.
                spark = SparkSession.builder.appName("ExportSparkSchema").enableHiveSupport().getOrCreate()

                print("Obtaining the database list...")
                # Obtain all databases.
                databases = spark.catalog.listDatabases()

                rows_data = []

                total_dbs = len(databases)
                print(f"{total_dbs} databases found. Traversing...")

                for db_index, db in enumerate(databases):
                    db_name = db.name
                    print(f"[{db_index + 1}/{total_dbs}] Processing database: {db_name}")

                    try:
                        # Obtain all tables in the current database.
                        tables = spark.catalog.listTables(db_name)

                        if not tables:
                            continue

                        for table in tables:
                            # Construct a complete table name (database_name.table_name).
                            full_table_name = f"{db_name}.{table.name}"

                            # Obtain the columns of the table.
                            # listColumns returns List[Column], including attributes such as name, dataType, and nullable.
                            columns = spark.catalog.listColumns(table.name, db_name)

                            for col in columns:
                                rows_data.append({
                                    "full_table_name": full_table_name,
                                    "column_name": col.name,
                                    "data_type": col.dataType
                                })

                    except Exception as e:
                        print(f"An error occurred when processing {db_name}: {str(e)}")
                        continue

                print(f"Data collection is complete. A total of {len(rows_data)} columns are collected.")

                # Write to the local CSV file.
                print(f"Writing to the local file: {output_path} ...")
                try:
                    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['full_table_name', 'column_name', 'data_type']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                        # Write the header row.
                        writer.writeheader()
                        # Write data rows.
                        writer.writerows(rows_data)

                    print("Exported successfully!")

                except IOError as e:
                    print(f"Failed to write to the file: {e}")

                # Stop the SparkSession.
                spark.stop()

            if __name__ == "__main__":
                # Define the output file path.
                output_file = "spark_table_schema.csv"

                # Check whether the file already exists to avoid issues caused by unintended append writes.
                if os.path.exists(output_file):
                    os.remove(output_file)

                export_spark_schema_to_csv(output_file)
            ```

## Quick Start

### Environment Setup

 1. Decompress the `BoostKit-omniruntime-omnihelper-*.zip` file.

     ```bash
     unzip BoostKit-omniruntime-omnihelper-*.zip
     ```

 2. Go to the extracted folder and extract the corresponding package.
     - Arm:

     ```bash
     tar -zxvf omnihelper_release_arm.tar.gz
     ```

     - x86:

     ```bash
     tar -zxvf omnihelper_release_x86.tar.gz
     ```

### Command Line Usage

**Syntax**

```bash
omnihelper [-h] --input_data INPUT_DATA [--output_dir OUTPUT_DIR]
           [--show-op-details] [--java-path JAVA_PATH] --class-path CLASS_PATH
```

**Parameter Description**

**Table 1** Basic parameters

|Parameter|Short Form|Mandatory/Optional|Description
|--|--|--|--|
|--help|-h|Optional|Displays help information.|
|--input_data|-i|Mandatory|Specifies the input path (a directory or a single file).<br>A single file in the `.lz4` or `.zstd` format can be directly processed.|
|--output_dir|-o|Optional|Specifies the output directory.<br>Default value: `./output`.|
|--show-op-details|-s|Optional|Hides the file sizes and output rows of the operators.|

**Table 2** Java parameters
|Parameter|Mandatory/Optional|Description

|--|--|--|
|--java-path|Optional|Specifies the Java executable file path. By default, `java` in the system `PATH` is invoked.|
|--class-path|Mandatory|Specifies the complete Java class path (including the parsed dependency JAR package).|

**Usage**

```bash
usage: omnihelper [-h] --input_data INPUT_DATA [--output_dir OUTPUT_DIR]
                  [--show-op-details] [--java-path JAVA_PATH] --class-path
                  CLASS_PATH

Big Data Operator Scanning Command Line Tool

optional arguments:
  -h, --help            show this help message and exit
  --input_data INPUT_DATA, -i INPUT_DATA
                        Input directory path or single file path (required).
                        If a single .lz4 or .zstd file is provided, only that
                        file will be processed.
  --output_dir OUTPUT_DIR, -o OUTPUT_DIR
                        Output directory path (default: ./output)
  --show-op-details, -s
                        Disable displaying op file sizes and output rows

Java Configuration:
  --java-path JAVA_PATH
                        Java executable path (default: "java" from system
                        PATH)
  --class-path CLASS_PATH
                        Complete Java classpath string
```

**Example**

**Example 1**: Parse a single log file.

     ```bash
     ./omnihelper -i ./input_data/eventlog.lz4 -o ./output_dir
     --java-path /path/to/java/bin/java
     --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
     ```

**Example 2**: Parse the log file directory.

     ```bash
     ./omnihelper -i ./input_dir -o ./output \
     --java-path /usr/local/jdk1.8/bin/java \
     --class-path /opt/omnihelper/resources/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/opt/spark-3.4.3-bin-hadoop3/jars/*
     ```

**Example 3**: Parse the log file directory and hide the file sizes and output rows of the operators.

     ```bash
     ./omnihelper -i ./input_dir -o ./output_dir -s
     --java-path /path/to/java/bin/java
     --class-path /path/to/boostkit-omnimv-logparser-spark-3.4.3-1.2.0-aarch64.jar:/path/to/spark-3.4.3-bin-hadoop3/jars/*
     ```

### Custom Function Configuration

You can customize function identification rules by modifying `resources/udf_dictionary.json`.<br>
Find the built-in file `udf_dictionary.json` in the `resources` directory and enter the specified functions to be identified. The format is as follows:

```json
[
    {
        "func_name": "int_plus_10",
        "is_support_func": false
    },
    {
        "func_name": "abs",
        "is_support_func": false
    }
]
```

Field description:

- `func_name`: Custom function name.
- `is_support_func`: indicates whether the function is supported. If the function needs to be identified, set it to `false`.

Priority rules:<br>
If a custom function has the same name as a built-in Spark function, it should be prioritized as a custom function.

**Analysis Report Parameters**

In the identification of unsupported expressions or built-in functions in Omni, the `Input` column by default represents the input parameter types of the function. Special parameter descriptions:

- cast function: The `Input` column represents the source parameter type and the target parameter type. For example, `cast(c_int as long)` returns the source parameter type `c_int` and the target parameter type `long`.
- if and case functions: The `Input` column represents the type of the result expression. For example, `if(a==1) 0 else 1` returns the type of the result `0`, which corresponds to the type of `a`.

## Security Declaration

### Routine Antivirus Software Check

Periodically scan clusters and Spark components for viruses. This protects clusters from viruses, malicious code, spyware, and malicious programs, reducing risks such as system breakdown and information leakage. Mainstream antivirus software is recommended for antivirus check.

### Log Control

- Check whether the system can limit the size of a single log file.
- Check whether there is a mechanism for clearing logs when the log space is used up.

### Vulnerability Fixing

To ensure the security of the production environment and reduce the risk of attacks, enable the firewall and periodically fix the following vulnerabilities:

- OS vulnerabilities
- JDK vulnerabilities
- Spark vulnerabilities
- Vulnerabilities in other components

    The following uses CVE-2021-37137 as an example.

    Vulnerability description:

    Netty 4.1.17 has two Content-Length HTTP headers that may be confused. The vulnerability ID is CVE-2021-37137.

    The system uses the hdfs-ceph (version 3.2.0) service as the storage object with decoupled storage and compute. This service depends on **aws-java-sdk-bundle-1.11.375.jar** and involves this vulnerability. You are advised to update the vulnerability patch in a timely manner to prevent hacker attacks.

    Impact:

    Netty 4.1.68 and earlier versions

    Handling suggestion:

    The vendor has released an upgrade patch to fix the vulnerability. For details, visit [GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363).

### SSH Hardening

During the installation and deployment, you need to connect to the server through SSH. The `root` user has all the operation permissions. Logging in to the server as the `root` user may pose security risks. You are advised to log in to the server as a common user for installation and deployment and disable `root` user login using SSH to improve system security.

Check the `PermitRootLogin` configuration item in `/etc/ssh/sshd\_config`.

- If the value is `no`, `root` user login using SSH is disabled.
- If the value is `yes`, change it to `no`.

## Disclaimers

**To OmniHelper users**

- This tool is intended solely for debugging and development. You are responsible for any risks and should carefully review the following information:
    - Data processing and deletion: Users are responsible for managing and deleting any data generated while using this tool. Users are advised to delete such data promptly after use to prevent information leakage.
    - Data confidentiality and transmission: Users understand and agree not to share or transmit any data generated by this tool. Neither the tool nor its developers are responsible for any information leaks, data breaches, or other negative consequences.
    - User input security: Users are responsible for the security of any commands they enter and for any risks or losses resulting from improper input. The tool and its developers are not liable for issues caused by incorrect command usage.

- Disclaimer scope: This disclaimer applies to all individuals and entities using this tool. By using the tool, you acknowledge and accept this statement and assume all risks and responsibilities arising from its use. If you do not agree, please stop using the tool immediately.
- Before using this tool, **please read and understand the preceding disclaimer**. If you have any questions, contact the developer.

**To data owners**

If you do not want your model or dataset to be mentioned in OmniHelper, or if you wish to update its description, please submit an issue on GitCode. We will delete or update your description according to your request. Thank you for your understanding and contribution to OmniHelper.

## License

The documents of this project are licensed under CC-BY 4.0. For details, see [LICENSE](docs/en/LICENSE).

## Contribution Statement

1. Submit an error report: If you find a non-security vulnerability in OmniHelper, first search the **Issues** in the OmniHelper repository to avoid submitting duplicates. If the vulnerability is not listed, create a new issue. If you discover a security-related problem, do not disclose it publicly. Please refer to the security handling guidelines for details. All error reports must include complete information about the issue.
2. Security issue handling: For guidance on handling security issues in this project, please contact the core team via email for instructions.
3. Resolving existing issues: Review the issue list of the repository to identify issues that need attention, and attempt to resolve them.
4. Proposing new features: Use the **Feature** label when creating an issue for a new feature. We will review and confirm proposals periodically.
5. How to contribute:
    1. Fork the repository of the project.
    2. Clone it to your local machine.
    3. Create a development branch.
    4. Conduct local testing. All unit tests, including any new test cases, must pass before submission.
    5. Submit your code.
    6. Create a pull request (PR).
    7. Code review: Modify the code according to review comments and resubmit your changes. This process may involve multiple rounds of iterations.
    8. After your PR is approved by the required number of reviewers, the committer will conduct the final review.
    9. After your PR is approved and all tests pass, the CI system will merge it into the project's main branch.

## Suggestions and Feedback

You are welcome to contribute to the community. If you have any questions or suggestions, please submit an [issue](https://gitcode.com/openeuler/OmniAdaptor). We will reply as soon as possible. Thank you for your support.

## Acknowledgments

Thank you to everyone in the community for your PRs. We warmly welcome contributions to OmniHelper!
