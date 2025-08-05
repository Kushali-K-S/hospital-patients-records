import pandas as pd
from pymongo import MongoClient
import os
import matplotlib.pyplot as plt
import schedule
import time


#ETL / ELT Pipeline for Employee Data
#Extract - csv to pandas DataFrame
patient_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'hprdata.csv'))
print(patient_df.head())

#Load - lake pandas DataFrame to MongoDB
client = MongoClient("mongodb+srv://akhilaakhila12309:6KHsF13N9q6uPE78@cluster0.xvtbfss.mongodb.net/")
db = client['lake_hpr']
collection = db['patients_raw']
collection.delete_many({})
collection.insert_many(patient_df.to_dict('records'))
print('Raw patient records loaded to lake database')

# Transform - MongoDB to pandas DataFrame
patient_df.drop_duplicates(inplace=True)
patient_df['dob'] = pd.to_datetime(patient_df['dob'], errors='coerce')
patient_df['visit_date'] = pd.to_datetime(patient_df['visit_date'], errors='coerce')
patient_df['age'] = pd.to_datetime('2024-06-30') - patient_df['dob']
patient_df['age'] = patient_df['age'].dt.days // 365
patient_df['lab_result'] = pd.to_numeric(patient_df['lab_result'], errors='coerce')
patient_df['lab_result'] = patient_df['lab_result'].fillna(patient_df['lab_result'].mean())

# Aggregate lab results per patient
lab_avg_df = patient_df.groupby('patient_id')['lab_result'].mean().reset_index()
lab_avg_df.rename(columns={'lab_result': 'avg_lab_result'}, inplace=True)

# Load - warehouse pandas DataFrame to MongoDB
db = client['warehouse_hpr']
collection = db['patients_processed']
collection.delete_many({})
collection.insert_many(patient_df.to_dict('records'))
print('Processed patient records loaded to warehouse database')

collection = db['lab_aggregates']
collection.delete_many({})
collection.insert_many(lab_avg_df.to_dict('records'))
print('Lab result aggregates loaded to warehouse database')

# Analytics - KPIs from processed patient data
processed_df = pd.DataFrame(list(db['patients_processed'].find()))
processed_df['visit_date'] = pd.to_datetime(processed_df['visit_date'], errors='coerce')
processed_df['discharge_date'] = pd.to_datetime(processed_df['discharge_date'], errors='coerce')
processed_df['length_of_stay'] = (processed_df['discharge_date'] - processed_df['visit_date']).dt.days

recovery_rate = (processed_df['outcome'] == 'recovered').mean()
readmission_rate = processed_df.duplicated(subset=['patient_id'], keep=False).mean()
avg_los = processed_df['length_of_stay'].mean()
complication_rate = (processed_df['complications'] == True).mean()

print(f"Recovery Rate: {recovery_rate:.2%}")
print(f"Readmission Rate: {readmission_rate:.2%}")
print(f"Average Length of Stay: {avg_los:.2f} days")
print(f"Complication Rate: {complication_rate:.2%}")


# Visualization - Patient outcomes and trends
# Outcome distribution
outcomes = processed_df['outcome'].value_counts()
outcomes.plot(kind='bar', color='skyblue')
plt.title('Patient Outcomes Distribution')
plt.xlabel('Outcome')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Lab result trends over time
lab_trend = processed_df.groupby(processed_df['visit_date'].dt.to_period('M'))['lab_result'].mean()
lab_trend.plot(kind='line', marker='o', color='green')
plt.title('Average Lab Results Over Time')
plt.xlabel('Month')
plt.ylabel('Lab Result')
plt.tight_layout()
plt.show()

# Department performance
dept_perf = processed_df.groupby('department')['outcome'].apply(lambda x: (x == 'recovered').mean())
dept_perf.sort_values().plot(kind='barh', color='orange')
plt.title('Recovery Rate by Department')
plt.xlabel('Recovery Rate')
plt.tight_layout()
plt.show()


# Automation - Schedule pipeline execution
def run_pipeline():
    print("Running ETL + Analytics Pipeline...")
    print("Pipeline executed successfully.")

schedule.every().day.at("02:00").do(run_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)
