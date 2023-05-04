# Data Engineering Zoomcamp Project
## Video Games Sales

##

This project aims to create a data pipeline using Video Games Sales dataset from Kaggle (https://www.kaggle.com/datasets/sidtwr/videogames-sales-dataset). The data consist of:

- Name: The games name
- Platform: Platform of the games release (i.e. PC,PS4, etc.)
- Year_of_Release: Year of the game's release
- Genre: Genre of the game
- Publisher: Publisher of the game
- NA_Sales: Sales in North America (in millions)
- EU_Sales: Sales in Europe (in millions)
- JP_Sales: Sales in Japan (in millions)
- Other_Sales: Sales in the rest of the world (in millions)
- Global_Sales: Total worldwide sales (in millions)
- Critic_Score: Aggregate score compiled by Metacritic staff
- Critic_Count: The number of critics used in coming up with the Critic_score
- User_Dcore: Score by Metacritic's subscribers
- User_Count: Number of users who gave the user_score
- Developer: Party responsible for creating the game
- Rating: The ESRB ratings

The data pipeline developed in this project can be summarized in 6 steps:
<ol>
<li> Set up the environment in Google Cloud using Terraform
<li> Use Prefect for workflow orchestration
<li> Upload data to Google Cloud Storage (Data lake)
<li> Move data from GC Storage to BigQuery (Data warehouse)
<li> Apply data transformations using dbt
<li> Create a dashboard using Google Looker Studio
</ol>




## Reproducibility:

In order to reproduce this project, apply the following steps:

- First of all, in order to be able to download data sets from Kaggle, run:
```
pip install kaggle
```
- Then, download your kaggle.json credentials from Kaggle API, and put that json file in the folder .kaggle/.


- Create a new folder for this project

- Copy the json file which includes your Google Cloud service account credentials into the folder

- Clone this GitHub repo:
```
git clone https://github.com/LadyTastingData/de-project.git
```

- Move to the folder de-project:
```
cd de-project
```


### Terraform:
In order to create the infrastructure of the project, Terraform can be used. For that, go to the folder terraform and run the following commands:
```
terraform init
terraform plan 
terraform apply
```

### Prefect:
Workflow orchestration was done with Prefect.
Go to the prefect folder and create a conda environment with name de-project, 
```
conda create -n de-project python=3.9
conda activate de-project
pip install -r requirements.txt
```

In another terminal window, run:
```
conda activate de-project
prefect orion start
```

Create Prefect GCS and GitHub blocks:
```
python make_gcp_blocks.py 
python make_gh_block.py
```
 
Download data from Kaggle and upload it to Google Cloud Storage:
```
python etl_web_to_gcs.py
```
Move the data from GC bucket to BigQuery:
```
python etl_gcs_to_bq.py
```

### Data transformations with dbt:


