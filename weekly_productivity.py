import pyodbc
import config
import pandas as pd
from datetime import datetime, timedelta

# Create the connection string
connection_string = f'DRIVER={{SQL Server}};SERVER={config.server};DATABASE={config.database};Trusted_Connection=yes;'

# Get the current date
current_date = datetime.now()

# Calculate @begindate as the first day of the current month
begindate = current_date.replace(day=1)

# Calculate @enddate as the last Friday before the current date
enddate = current_date - timedelta(days=(current_date.weekday() - 4) % 7)

# Convert dates to strings in the format 'mm/dd/yyyy'
begindate_str = begindate.strftime('%m/%d/%Y')
enddate_str = enddate.strftime('%m/%d/%Y')

sql_code = f'''
DECLARE @begindate date = '{begindate_str}'
DECLARE @enddate date = '{enddate_str}'
SELECT 
	loc.department_name AS Loc_Name,
	prov.prov_id AS Prov_ID,
	prov.name AS Prov_Name,
	(SUM(CASE WHEN astat.name = 'Completed' THEN 1 ELSE 0 END)) AS Checked_Out,
	SUM(CASE WHEN astat.name = 'No Show' THEN 1 ELSE 0 END) AS No_Shows,
	FORMAT((SUM(CASE WHEN astat.name = 'No Show' THEN 1 ELSE 0 END) / CAST(COUNT(enc.pat_enc_csn_id) AS float)), 'P') AS "No_Show_%",
	(SUM(CASE WHEN zcr.name IS NOT NULL THEN 1 ELSE 0 END) 
		- (SUM(CASE WHEN zcr.name = 'Late Cancel' THEN 1 ELSE 0 END) 
			+ SUM(CASE WHEN zcr.name LIKE '%Reschedule%' OR astat.name LIKE '%Reschedule%'THEN 1 ELSE 0 END))) AS Canceled,
	SUM(CASE WHEN zcr.name = 'Late Cancel' THEN 1 ELSE 0 END) AS Late_Canceled,
	SUM(CASE WHEN zcr.name LIKE '%Reschedule%' OR astat.name LIKE '%Reschedule%'THEN 1 ELSE 0 END) AS Rescheduled,
	COUNT(enc.pat_enc_csn_id) AS Scheduled_Appts,
	SUM(CASE WHEN DATEDIFF(d, enc.appt_made_date, enc.contact_date) = 0 THEN 1 ELSE 0 END) AS Walk_Ins,
	FORMAT((SUM(CASE WHEN DATEDIFF(d, enc.appt_made_date, enc.contact_date) = 0 THEN 1 ELSE 0 END) / CAST(COUNT(enc.pat_enc_csn_id) AS float)), 'P') AS "Walk_Ins_%"
FROM clarity_emp_view prov
	INNER JOIN clarity_ser_2 AS cs
		ON cs.prov_id = prov.prov_id
	LEFT JOIN pat_enc_view AS enc
        ON prov.prov_id = enc.visit_prov_id
    LEFT JOIN clarity_dep_view AS loc
		ON enc.department_id = loc.department_id
    LEFT JOIN zc_appt_status AS astat
	    ON enc.appt_status_c = astat.appt_status_c
	LEFT JOIN patient AS pat
    	ON enc.pat_id = pat.pat_id
	LEFT JOIN zc_cancel_reason AS zcr
    	ON enc.cancel_reason_c = zcr.cancel_reason_c
WHERE cs.npi IS NOT NULL
	AND enc.department_id IN ('246001002', '246002001', '246003002', '246004002', '246006002')
	AND prov.user_status_c = '1'
	AND enc.contact_date BETWEEN @begindate AND @enddate
	AND enc.appt_prc_id IS NOT NULL
	AND (enc.cancel_reason_c != '4' OR enc.cancel_reason_c IS NULL)
	AND prov.prov_id != '246206'  -- tamara strong chavez
GROUP BY prov.prov_id,
	prov.name,
	loc.department_name
ORDER BY loc.department_name, prov.name
'''

try:
    # Connect to the database
    conn = pyodbc.connect(connection_string)

    # Create a cursor
    cursor = conn.cursor()

    # Execute a sample query
    cursor.execute(sql_code)

    # Fetch and append the query results to a dataframe
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
 
    # Convert each Row to a list and collect all rows into a new list
    rows_list = [list(row) for row in rows]

    # Pass rows_list instead of rows
    df = pd.DataFrame(rows_list, columns=column_names)

    # Remove rows where 'Checked_Out' equals 0
    df.drop(df[df['Checked_Out'] == 0].index, inplace=True)

    # Close the cursor and connection
    cursor.close()
    conn.close()

except pyodbc.Error as e:
    print(f'Error connecting to SQL Server: {str(e)}')

df['No_Show_%'] = df['No_Show_%'].str.rstrip('%').astype('float') / 100.0
df['Walk_Ins_%'] = df['Walk_Ins_%'].str.rstrip('%').astype('float') / 100.0
summary = df.groupby('Loc_Name').agg({
    'Checked_Out': 'sum',
    'No_Shows': 'sum',
    'No_Show_%': 'mean',
    'Canceled': 'sum',
    'Late_Canceled': 'sum',
    'Rescheduled': 'sum',
    'Scheduled_Appts': 'sum',
    'Walk_Ins': 'sum',
    'Walk_Ins_%': 'mean'
})
summary['No_Show_%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in summary['No_Show_%']], index = summary.index)
summary['Walk_Ins_%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in summary['Walk_Ins_%']], index = summary.index)
df['No_Show_%'] = (df['No_Show_%'] * 100).map('{:.2f}%'.format)
df['Walk_Ins_%'] = (df['Walk_Ins_%'] * 100).map('{:.2f}%'.format)

# Reset the index
summary = summary.reset_index()

with pd.ExcelWriter('test.xlsx') as writer:
    # Write the title and dates
    title_df = pd.DataFrame({'Report': ['BEHAVIORAL HEALTH: MONTHLY SUMMARY OF DAILY PRODUCTIVITY TRACKING'], 
                             'Start Date': [begindate_str], 
                             'End Date': [enddate_str]})
    title_df.to_excel(writer, index=False)

    # Initialize the current row number
    row = len(title_df) + 2  # We add 2 to leave a blank row after the title and dates

    for loc_name in df['Loc_Name'].unique():
        # Extract the rows for the current loc_name
        df_loc = df[df['Loc_Name'] == loc_name]

        # Write the rows to the Excel file, starting from the current row
        df_loc.to_excel(writer, startrow=row, index=False)

        # Update the current row number
        row += len(df_loc) + 1  # We add 1 to leave a blank row after each group of rows
        
        # Extract the summary for the current loc_name
        summary_loc = summary[summary['Loc_Name'] == loc_name]
        
        writer.sheets['Sheet1'].write(row, 0, 'Total')

        # Write the summary to the Excel file, starting from the current row
        summary_loc.to_excel(writer, startrow=row, startcol=2, index=False, header=False)
        
        # Update the current row number
        row += len(summary_loc) + 2  # We add 2 to leave a blank row after each summary

