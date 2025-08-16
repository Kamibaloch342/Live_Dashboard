import pandas as pd
import os
import json
import re
from datetime import datetime, time

# Define the project-level targets based on your business logic
MONTHLY_TARGET_PER_TRAINER = 2048 / 8  # 256 beneficiaries per month
TOTAL_PROJECT_TARGET_BENEFICIARIES = 250000

def standardize_cnic(cnic_str):
    """Removes all non-digit characters from a CNIC string for consistent comparison."""
    if pd.notna(cnic_str):
        return re.sub(r'[^0-9]', '', str(cnic_str))
    return ''

def prepare_dashboard_data(master_file_path, output_json_path):
    """
    Reads the master workflow Excel file, cleans and aggregates the data
    to produce a JSON file for the dashboard.
    """
    if not os.path.exists(master_file_path):
        print(f"Error: The master file '{master_file_path}' was not found.")
        return

    try:
        cnic_cols = [f'beneficiary_cnic_{i}' for i in range(1, 36)]
        cnic_dtype_map = {col: str for col in cnic_cols}
        
        print(f"Reading data from '{master_file_path}' with explicit data types...")
        df_master = pd.read_excel(master_file_path, dtype=cnic_dtype_map)
        
        df_day2 = df_master[df_master['main_menu'] == 'day2'].copy()
        day2_attendance = {}
        for _, row in df_day2.iterrows():
            day2_session_id = row.get('day2_session_select')
            beneficiaries_present = row.get('beneficiaries_present')
            if pd.notna(day2_session_id) and pd.notna(beneficiaries_present):
                cnic_list = {standardize_cnic(c) for c in str(beneficiaries_present).split(' ') if c.strip()}
                day2_attendance[day2_session_id] = cnic_list

        long_format_list = []
        df_day1 = df_master[df_master['main_menu'] == 'day1'].copy()
        
        beneficiary_specific_prefixes = ['beneficiary_name', 'beneficiary_cnic', 'age', 'occupation']
        
        for _, row in df_day1.iterrows():
            session_data = {
                'SubmissionDate': row.get('SubmissionDate'),
                'start': row.get('start'),
                'end': row.get('end'),
                'province_select': row.get('province_select'),
                'district_select': row.get('district_select'),
                'lead_trainer_name': row.get('lead_trainer_name'),
                'session_id': row.get('session_id'),
                'training_location': row.get('training_location'),
                'q_read_write': row.get('q_read_write', 0),
                'q_recognize_currency': row.get('q_recognize_currency', 0),
                'q_simple_math': row.get('q_simple_math', 0),
                'q_handbooks_distributed': row.get('q_handbooks_distributed', 0)
            }
            
            for i in range(1, 36):
                cnic_col = f'beneficiary_cnic_{i}'
                if cnic_col in df_day1.columns and pd.notna(row.get(cnic_col)):
                    beneficiary_data = session_data.copy()
                    
                    beneficiary_data['beneficiary_cnic'] = standardize_cnic(row.get(cnic_col))
                    for prefix in beneficiary_specific_prefixes:
                        source_col = f'{prefix}_{i}'
                        beneficiary_data[prefix] = row.get(source_col)
                    
                    long_format_list.append(beneficiary_data)

        df_final_database = pd.DataFrame(long_format_list)
        print(f"✅ Created the final long-format database with {len(df_final_database)} rows.")

        dashboard_records = []
        grouped_by_session = df_final_database.groupby('session_id')
        
        for session_id, group in grouped_by_session:
            first_row = group.iloc[0]
            
            lat, lon = None, None
            location_str = str(first_row.get('training_location'))
            if pd.notna(location_str) and location_str.strip() not in ['nan', '']:
                try:
                    coords = location_str.split(' ')
                    lat, lon = float(coords[0]), float(coords[1])
                except (ValueError, IndexError):
                    pass
            
            beneficiary_count_day1 = group['beneficiary_cnic'].count()
            cnics_day1 = set(group['beneficiary_cnic'].astype(str).tolist())
            cnics_day2 = set(day2_attendance.get(session_id, set()))
            retention_rate = (len(cnics_day2) / beneficiary_count_day1) * 100 if beneficiary_count_day1 > 0 else 0
            
            start_dt, end_dt = None, None
            try:
                start_dt = pd.to_datetime(re.search(r'\d{2}:\d{2}:\d{2}', str(first_row.get('start'))).group(), format='%H:%M:%S').time()
                end_dt = pd.to_datetime(re.search(r'\d{2}:\d{2}:\d{2}', str(first_row.get('end'))).group(), format='%H:%M:%S').time()
                duration_td = datetime.combine(datetime.min, end_dt) - datetime.combine(datetime.min, start_dt)
                avg_training_time_hours = duration_td.total_seconds() / 3600
            except (ValueError, AttributeError):
                avg_training_time_hours = 0
            
            quality_score = (first_row.get('q_read_write', 0) + first_row.get('q_recognize_currency', 0) + first_row.get('q_simple_math', 0)) / 3
            
            dashboard_records.append({
                "TrainingID": session_id,
                "Training_Date": str(first_row.get('SubmissionDate')).split(' ')[0],
                "Province": first_row.get('province_select'),
                "District": first_row.get('district_select'),
                "ASPC_Name": first_row.get('lead_trainer_name'),
                "Beneficiary_Count_Actual": int(beneficiary_count_day1),
                "Female_Beneficiaries": int(beneficiary_count_day1), # Assuming all beneficiaries are female
                "Retention_Rate_Pct": round(retention_rate, 2),
                "Quality_Index": round(quality_score, 2),
                "Avg_Training_Time_Hours": round(avg_training_time_hours, 1),
                "Female_Occupations": group['occupation'].dropna().value_counts().to_dict(),
                "Start_Location_Lat": lat,
                "Start_Location_Lon": lon
            })
            
        final_json = {
            "dashboard_data": dashboard_records,
            "metadata": {
                "TOTAL_PROJECT_TARGET_BENEFICIARIES": TOTAL_PROJECT_TARGET_BENEFICIARIES,
                "MONTHLY_TARGET_PER_TRAINER": MONTHLY_TARGET_PER_TRAINER
            }
        }

        with open(output_json_path, 'w') as f:
            json.dump(final_json, f, indent=4)
        
        print(f"\n🎉 Successfully created dashboard data! Saved to '{output_json_path}'.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    # Make sure this path points to your actual Excel file
    master_excel_path = r"C:\Users\kamib\Downloads\Survecto_Phase2_DFLT-2_Master_Workflow.xlsx"
    output_json_path = 'dashboard_data.json'
    
    prepare_dashboard_data(master_excel_path, output_json_path)