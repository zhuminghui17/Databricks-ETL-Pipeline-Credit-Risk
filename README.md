[![CI](https://github.com/zhuminghui17/python-template/actions/workflows/cicd.yml/badge.svg)](https://github.com/zhuminghui17/python-template/actions/workflows/cicd.yml)



# Project #3: Databricks ETL (Extract Transform Load) Pipeline in Credit Card Approval

## Overview

This project, featuring a complete ETL (Extract, Transform, Load) pipeline utilizing Databricks, Delta Lake, and SparkSQL, is centered around managing credit card approval data. It includes processes for extracting this data, optimizing it through transformation for better analytical clarity, and loading it into a user-friendly format. The inclusion of data visualization techniques further augments the utility of this pipeline. 

In real-world business scenarios, such a pipeline can significantly aid in decision-making processes by providing clear, actionable insights from complex data. It can help in identifying trends in credit card approvals, pinpointing areas for operational improvement, and ensuring compliance with regulatory standards. Moreover, by enabling more efficient data handling and analysis, businesses can enhance customer experience, streamline risk management, and drive strategic growth.

## Databricks Concepts
- **Databricks Notebook**: An interactive web interface for data processing and visualization, supporting multiple programming languages. Useful for developing, testing, and collaborating on data pipelines and analyses.

- **Delta Lake**: An open-source storage layer for Data Lakes, providing ACID transactions and unifying streaming and batch data processing. Enhances data reliability and integrity, especially in large-scale environments.

- **SparkSQL**: A module in Apache Spark for processing structured data using SQL or DataFrame APIs. It enables efficient big data processing and supports various data sources, ideal for data exploration and pipeline creation.

- **Automation Trigger**: Mechanisms in Databricks that automatically initiate workflows or processes based on schedules, events, or conditions, improving operational efficiency and enabling timely data processing.


## Dataset

The Credit Card Approval dataset from the University of California Irvine (UCI) Machine Learning Repository is used to build automatic credit card approval predictors. The dataset is multivariate, with categorical, integer, and real feature types. It has 690 instances, with 55.5% of applications being denied and 44.5% being approved.

## How to run
1. Create a Databricks Cluster in the `Compute` Section and a Workspace on Azure
2. Connect the GitHub account to Databricks Workspace
    - Also add Databricks Extension on GitHub
3. In the Databricks workspace, clone the GitHub repo into Databricks
4. Run the notebooks
    - `Extract Data.ipynb` 
    - `Transform Data.ipynb` 
    - `Visualize Data.ipynb` 

## ETL Pipeline

### 1. Extract Data
Upload data to DBFS, use below code to extract data. 
![Alt text](/images/image.png)

### 2. Validate Data
1. Missing Values: Identify and handle missing values in the dataset.
2. Data Types: Ensure that each column has the appropriate data type.
3. Range Checks: Validate that numerical features fall within expected ranges.
4. Categorical Data Consistency: Ensure that categorical values are consistent and as expected.
5. Unique Values: Check for uniqueness where applicable (e.g., IDs).
6. Outliers: Detect and handle any outliers in numerical data.

![Alt text](/images/image-1.png)
![Alt text](/images/image-2.png)


### 3. Transform Data
Utilize Spark SQL to execute data transformations.
![Alt text](/images/image-3.png)

### 4. Load Data in Delta Lake
Use Delta Lake for efficient and reliable data storage.
![Alt text](/images/image-5.png)

### 5. Visualize Data
Make data visualization for intuitive insights
![Alt text](/images/image-6.png)
![Alt text](/images/image-7.png)
![Alt text](/images/image-8.png)

## The Usage of Delta Lake

Delta Lake offers several key benefits:

- **Reliability for Data Lakes**: It ensures data integrity with ACID transactions even for massive datasets.
- **Unified Batch and Streaming**: Simplifies processing by treating streaming and batch data as the same.
- **Scalable Metadata Handling**: Efficiently manages metadata for large-scale datasets, enhancing performance.
- **Schema Enforcement and Evolution**: Maintains data quality with schema validation and allows for schema changes over time.

![Alt text](/images/image-9.png)

## Automated Pipeline and Trigger
After setting up the trigger, the ETL pipeline will run automatically according to the schedule which is specified. 
![Alt text](/images/image-10.png)
![Alt text](/images/image-11.png)

## Conclusion and Actionable, data-driven recommendation
- **Review and Adjust Credit Approval Criteria**:

    The markedly lower approval rate for the "Latino" group suggests a need to review the credit approval criteria to ensure they are equitable and not inadvertently discriminatory. Assess whether certain criteria are disproportionately disadvantaging this group and adjust accordingly.

- **Enhance Financial Inclusion Programs**:

    Develop financial literacy programs targeted at ethnic groups with lower approval rates, such as "Latino" and "Asian". These programs can help potential applicants understand how to improve their credit scores, manage debt, and become more creditworthy.

- **Ensure Compliance with Fair Lending Laws**:

    Conduct an internal audit to ensure that the lending process is compliant with fair lending laws and that there are no biases in the approval process. Ensuring compliance not only is legally mandatory but also helps in building a reputation for fairness and ethical business practices.

    
## Requirements
Your project should include the following:

- A well-documented Databricks notebook that performs ETL (Extract, Transform, Load) operations, checked into the repository.
- Usage of Delta Lake for data storage.
- Usage of Spark SQL for data transformations.
- Proper error handling and data validation.
- Visualization of the transformed data.
- An automated trigger to initiate the pipeline.

- README.md: A file that clearly explains what the project does, its dependencies, how to run the program, and concludes with actionable and data-driven recommendations to a hypothetical management team.	
- Video Demo: A YouTube link in README.md showing a clear, concise walkthrough and demonstration of your ETL pipeline, including the automated trigger and recommendations to the management team.
	


## Grading Rubric

Your project will be graded on the following criteria:
- Databricks ETL Pipeline (20 points): Your Databricks notebook correctly extracts data, performs transformations, and loads the data into a final output.
    - Extract operation (7 points): Your notebook correctly extracts data from the source data.
    - Transform operation (7 points): Your notebook correctly transforms the extracted data.		
    - Load operation (6 points): Your notebook correctly loads the transformed data into the destination data store.
		
- Usage of Delta Lake (20 points): Your ETL pipeline correctly utilizes Delta Lake for data storage.
	
- Correct setup and usage of Delta Lake (10 points): Your notebook correctly sets up Delta Lake and uses it to store the transformed data.

- Data validation checks (10 points): Your notebook includes data validation checks to ensure the quality of the data.
		
- Usage of Spark SQL (20 points): Your pipeline effectively utilizes Spark SQL for data transformations.

- Correct use of Spark SQL (10 points): Your notebook correctly uses Spark SQL to perform the data transformations.
				
- Effectiveness of data transformations (10 points): The data transformations are effective in cleaning and preparing the data for analysis.
			
- Visualization and Conclusion (15 points): Your project includes a visualization of the transformed data and a data-driven, actionable recommendation for a management team, both in the notebook and README.md.

- Quality of data visualization (7 points): The visualization is clear, concise, and effective in communicating the results of the data analysis.
		
- Actionable, data-driven recommendation (8 points): The recommendation is based on the results of the data analysis and is actionable by the management team.
		
	
- README.md (10 points): The README.md file is clear, concise, and guides the user on how to run the program and includes the conclusion and recommendation.

    - Clarity and completeness (5 points): The README.md file is easy to read and understand and includes all the information necessary to run the program.
                    
    - Inclusion of conclusion and recommendation (5 points): The README.md file includes the conclusion and recommendation from the project.
		
			
- Automated Trigger (10 points): Your project includes an automated trigger to initiate the pipeline, demonstrated in the video.

		
- Correct setup of the trigger (5 points): The trigger is correctly set up to initiate the pipeline on a schedule or event.
				
			
- Effective demonstration in video (5 points): The video demonstrates the correct setup and usage of the trigger.
			
	
Demo Video (5 points): A 2-5 minute video explaining the project and demonstrating its functionality is included. The video should be high-quality (both audio and visual), not exceed the given time limit, and be linked in the README via a private or public YouTube link.

- Clarity of explanation (2 points): The video clearly explains the project and its functionality.
						
- Quality demonstration of the project (2 points): The video demonstrates the project and its functionality in a clear and concise manner.
						
- Quality of video and audio (1 point): The video is high-quality and easy to understand.