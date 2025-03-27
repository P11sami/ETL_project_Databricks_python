[![Open in Codespaces](https://classroom.github.com/assets/launch-codespace-2972f46106e565e64193e422d61a12cf1da4916b45550586e14ef0a7c637dd04.svg)](https://classroom.github.com/open-in-codespaces?assignment_repo_id=18811843)
# Data-driven Computing Architectures 2025 - final project - Sami Seppälä

## 1 - Overview

This project implements a data pipeline for processing Formula 1 racing data using Databricks and Medallion Architecture (Bronze → Silver → Gold).

## 2- Contributions

This project was done alone by me, Sami Seppälä

## 3 - Description

This project investigates the Formula 1 World Championship (1950 - 2024) data found on [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020). The data consists of 14 csv files consisting of all information on the Formula 1 races, drivers, constructors, qualifying, circuits, lap times, pit stops, championships from 1950 till the latest 2024 season. This data is utalized in a reusable ETL pipeline where the data is cleaned and aggregated into a single table where race results are sorted by date.

## 4 - Pipeline ovreview

The pipeline follows the Medallion Architecture:

Bronze Layer: Ingesting of raw data.

Silver Layer: Standardization, deduplication, metadata.

Gold Layer: Aggregation, joining.

## 5 - Repository Structure

📂
- [README.md](https://github.com/aa-it-vasa/ddca2025-project-group_60/blob/main/README.md) - Main project report (this file)  
- [code/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/code) - Pipeline code, test code, and visualization code  
- [docs/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/docs) - Project documentation  
- [misc/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/misc) - Empty  
- [data/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/data) - Data used  
- [test/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/test) - Unit test code  
- [example/](https://github.com/aa-it-vasa/ddca2025-project-group_60/tree/main/example) - Example of working pipeline

## 6 - Example

To demonstrate the pipleine you can check the [example](example/Example of working pipeline.ipynb) notebook. The notebook contains the same pipleine code from the [code](code/Final_project.ipynb) folder. The unit tests from the [test](test/Test_script.ipynb) folder are then ran to ensure all necesarry steps in the pipeline have been performed correctly.

![image](https://github.com/user-attachments/assets/82435ca8-663f-4991-947b-6f93c424771f)
All tests passed.

The visualizations from the [code](code/Visualizations.ipynb) folder are then performed to gain insight into driver performance.

Top 5 drivers by race wins

![image](https://github.com/user-attachments/assets/4eb8c61a-d33d-4622-9662-b0f8e459b45d)

Top 5 drivers by qualifying wins

![image](https://github.com/user-attachments/assets/61616796-43db-4b90-aaf6-5e2a9fff4c31)

Top 5 constructors by winrate

![image](https://github.com/user-attachments/assets/738626cd-8dd8-4ae3-b902-4ef121319a40)

Top 5 circuits by DNF rate

![image](https://github.com/user-attachments/assets/60bb5883-7360-4b57-884e-fd67939f7b45)

Correlation between driver nationality and race wins

![image](https://github.com/user-attachments/assets/493d5613-1b57-4d31-b22a-5f29cbe86ed2)

Wins by nationality normalized for driver count

![image](https://github.com/user-attachments/assets/2d29aafb-fa65-4fa3-9dfa-679ccba04e4b)

## 8 - Documentation

-
-
-
## 9 - Conclusion

This project builds a reusable data pipeline for Formual 1 race data using the medallion sctructure. The data is used to gain insight on race winners and create visualizations for different metrics.

## 10 - References

- [Kaggle data](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)

- [Databricks documentation](https://docs.databricks.com/aws/en)
