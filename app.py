from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from werkzeug.security import check_password_hash, generate_password_hash
import sqlite3

app = Flask(__name__)
app.secret_key = 'your_secret_key_change_this_in_production'

# Google Sheets Configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# You'll need to replace these with your actual Google Sheets IDs
USERS_SHEET_ID = '1NTpUUA1pomN2X5-HVgxzdIBjE5XMY6bT8pWTr0AnmhI'  # Replace with your users sheet ID
REPORTS_SHEET_ID = '1neLmPxMe-NKeuH0gVwzE9FjdpvPgNIUWql6Ez7aYh8Q'  # Replace with your reports sheet ID

# Service account credentials file path
CREDENTIALS_FILE = 'service_account_credentials.json'

DB_FILE = 'zila_data.db'

def ensure_monthly_reports_columns():
    # List of all columns your code expects in monthly_reports
    required_columns = [
        'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
        'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
        'alaqajat_start', 'alaqajat_end', 'alaqajat_target','alaqajat_izafa','alaqajat_kami','alaqajat_ikhtitaam',
        'halqajat_start', 'halqajat_end', 'halqajat_target','halqajat_izafa','halqajat_kami','halqajat_ikhtitaam',
        'halqajat_ward_start', 'halqajat_ward_end', 'halqajat_ward_target','halqajat_ward_izafa','halqajat_ward_kami','halqajat_ward_ikhtitaam',
        'block_code_start', 'block_code_end', 'block_code_target','block_code_izafa','block_code_kami','block_code_ikhtitaam',
        'arkaan_start', 'arkaan_end', 'arkaan_target','arkaan_izafa','arkaan_kami','arkaan_ikhtitaam',
        'umeedwaran_start', 'umeedwaran_end', 'umeedwaran_target','umeedwaran_izafa','umeedwaran_kami','umeedwaran_ikhtitaam',
        'hangami_start', 'hangami_end', 'hangami_target','hangami_izafa','hangami_kami','hangami_ikhtitaam',
        'muawanin_start', 'muawanin_end', 'muawanin_target','muawanin_izafa','muawanin_kami','muawanin_ikhtitaam',
        'mutayyin_afrad_start', 'mutayyin_afrad_end', 'mutayyin_afrad_target','mutayyin_afrad_izafa','mutayyin_afrad_kami','mutayyin_afrad_ikhtitaam',
        'member_start', 'member_end', 'member_target','member_izafa','member_kami','member_ikhtitaam',
        'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'youth_programat_name',
        'zilai_shura_planned', 'zilai_shura_held', 'zilai_shura_attendance',
        'nazm_zila_planned', 'nazm_zila_held', 'nazm_zila_attendance',
        'nazimin_alaqajat_planned', 'nazimin_alaqajat_held', 'nazimin_alaqajat_attendance',
        'zilai_ijtima_arkaan_planned', 'zilai_ijtima_arkaan_held', 'zilai_ijtima_arkaan_attendance',
        'zilai_ijtima_umeedwaran_planned', 'zilai_ijtima_umeedwaran_held', 'zilai_ijtima_umeedwaran_attendance',
        'ijtima_arkaan_alaqah_planned', 'ijtima_arkaan_alaqah_held', 'ijtima_arkaan_alaqah_attendance',
        'ijtima_umeedwaran_alaqah_planned', 'ijtima_umeedwaran_alaqah_held', 'ijtima_umeedwaran_alaqah_attendance',
        'ijtima_karkunaan_alaqah_planned', 'ijtima_karkunaan_alaqah_held', 'ijtima_karkunaan_alaqah_attendance',
        'ijtima_karkunaan_halqajat_planned', 'ijtima_karkunaan_halqajat_held', 'ijtima_karkunaan_halqajat_attendance',
        'ijtima_nazimin_halqajat_planned', 'ijtima_nazimin_halqajat_held', 'ijtima_nazimin_halqajat_attendance',
        'dars_quran_planned', 'dars_quran_held', 'dars_quran_attendance',
        'dawati_camp_planned', 'dawati_camp_held', 'dawati_camp_attendance',
        'gharon_tak_dawat_planned', 'gharon_tak_dawat_held', 'gharon_tak_dawat_attendance',
        'taqseem_literature_planned', 'taqseem_literature_held', 'taqseem_literature_attendance',
        'amir_zila_maqamat', 'amir_zila_daurajat', 'amir_zila_mulaqat',
        'qaim_zila_maqamat', 'qaim_zila_daurajat', 'qaim_zila_mulaqat',
        'naib_amir_zila_maqamat', 'naib_amir_zila_daurajat', 'naib_amir_zila_mulaqat',
        'study_circle_maqamat', 'study_circle_daurajat', 'study_circle_attendance',
        'quran_course_maqamat', 'quran_course_daurajat', 'quran_course_attendance',
        'retreat_maqamat', 'retreat_daurajat', 'retreat_attendance',
        'quran_courses', 'quran_classes', 'quran_participants',
        'fahem_quran_attendance', 'quran_target', 'quran_distributed',
        'central_training_target', 'central_training_actual', 'other_trainings',
        'atifal_programs', 'awaami_committees', 'awaami_committees_count',
        'koi_or_bat',
    ]
    # Types for each column (default to INTEGER, TEXT for *_activities)
    text_columns = {'youth_activities'}
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('PRAGMA table_info(monthly_reports)')
    existing_cols = [row[1] for row in c.fetchall()]
    for col in required_columns:
        if col not in existing_cols:
            col_type = 'TEXT' if col in text_columns else 'INTEGER'
            try:
                c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} {col_type}')
                print(f"Added column {col} to monthly_reports")
            except Exception as e:
                print(f"Error adding column {col}: {e}")
    conn.commit()
    conn.close()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Users table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            zila TEXT NOT NULL,
            role TEXT NOT NULL
        )
    ''')
    # Monthly reports table (fields trimmed for brevity, add all needed fields)
    c.execute('''
        CREATE TABLE IF NOT EXISTS monthly_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zila TEXT NOT NULL,
            month TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            submitted_by TEXT NOT NULL,
            union_committee_count INTEGER,
            wards_count INTEGER,
            block_code_count INTEGER,
            cantonment_board_count INTEGER,
            nazm_qaim_union INTEGER,
            nazm_qaim_wards INTEGER,
            nazm_qaim_blockcode INTEGER,
            nazm_qaim_cantonment INTEGER
            -- Add all other fields here, matching the Google Sheets columns
        )
    ''')
    conn.commit()
    conn.close()
    ensure_monthly_reports_columns()

# Call this at the start of the app
init_db()

def load_users_from_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT username, password, zila, role FROM users')
    rows = c.fetchall()
    conn.close()
    if not rows:
        # If DB is empty, insert default users
        df = create_default_users()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        for _, row in df.iterrows():
            try:
                c.execute('INSERT INTO users (username, password, zila, role) VALUES (?, ?, ?, ?)',
                          (row['username'], row['password'], row['zila'], row['role']))
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        conn.close()
        return df
    else:
        import pandas as pd
        return pd.DataFrame(rows, columns=['username', 'password', 'zila', 'role'])

def create_default_users():
    """Create default users DataFrame"""
    default_users = pd.DataFrame({
        'username': [
            'admin', 
            'airport', 'gadap', 'gharbi', 'wasti', 'gulberg_wasti', 
            'junoobi', 'keemari', 'korangi', 'malir', 'quaideen', 
            'sharqi', 'shumali', 'site_gharbi'
        ],
        'password': [
            'admin123',
            'airport123', 'gadap123', 'gharbi123', 'wasti123', 'gulberg123',
            'junoobi123', 'keemari123', 'korangi123', 'malir123', 'quaideen123',
            'sharqi123', 'shumali123', 'site123'
        ],
        'zila': [
            'کراچی مرکز',
            'ایئرپورٹ', 'گڈاپ', 'غربی', 'وسطی', 'گلبرگ وسطی',
            'جنوبی', 'کیماڑی', 'کورنگی', 'ملیر', 'قائد اعظم',
            'شرقی', 'شمالی', 'سائٹ غربی'
        ],
        'role': [
            'admin',
            'user', 'user', 'user', 'user', 'user',
            'user', 'user', 'user', 'user', 'user',
            'user', 'user', 'user'
        ]
    })
    return default_users

def load_reports_from_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT * FROM monthly_reports')
    rows = c.fetchall()
    columns = [desc[0] for desc in c.description]
    conn.close()
    import pandas as pd
    return pd.DataFrame(rows, columns=columns)

def save_report_to_db(new_entry):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Only insert columns that exist in the table
    keys = list(new_entry.keys())
    values = [new_entry[k] for k in keys]
    placeholders = ','.join(['?'] * len(keys))
    sql = f"INSERT INTO monthly_reports ({','.join(keys)}) VALUES ({placeholders})"
    c.execute(sql, values)
    conn.commit()
    conn.close()

def update_report_in_db(zila, month, new_entry):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    set_clause = ', '.join([f'{k}=?' for k in new_entry.keys()])
    values = list(new_entry.values())
    values.extend([zila, month])
    sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND month=?"
    c.execute(sql, values)
    conn.commit()
    conn.close()

# Load initial data
users_df = load_users_from_db()
report_df = load_reports_from_db()

def get_form_data(zila, current_month):
    """Get data for form fields from SQLite DB"""
    try:
        # Reload data from DB
        global report_df
        report_df = load_reports_from_db()
        # Calculate previous month
        current_date = datetime.strptime(current_month, '%Y-%m')
        prev_month = (current_date - timedelta(days=1)).strftime('%Y-%m')
        print(f"Looking for data for zila: {zila}")
        print(f"Current month: {current_month}")
        print(f"Previous month: {prev_month}")
        # Get previous month data
        prev_data = report_df[(report_df['zila'] == zila) & (report_df['month'] == prev_month)]
        print(f"Previous month data found: {len(prev_data)} rows")
        # Get current month data
        current_data = report_df[(report_df['zila'] == zila) & (report_df['month'] == current_month)]
        print(f"Current month data found: {len(current_data)} rows")
        form_data = {}
        # Default values for آغاز and ہدف
        default_values = {
            'alaqajat_start': 150, 'alaqajat_target': 200,
            'halqajat_start': 300, 'halqajat_target': 400,
            'halqajat_ward_start': 250, 'halqajat_ward_target': 300,
            'block_code_start': 100, 'block_code_target': 150,
            'arkaan_start': 150, 'arkaan_target': 200,
            'umeedwaran_start': 300, 'umeedwaran_target': 400,
            'members_start': 250, 'members_target': 300,
            'muawanin_start': 100, 'muawanin_target': 150,
            'mutayyin_afrad_start': 100, 'mutayyin_afrad_target': 150,
        }
        # If we have current month data, use it
        if not current_data.empty:
            first_row = current_data.iloc[0]
            form_data = first_row.to_dict()
            # Convert NaN to empty string for all fields
            form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
        # If we have previous month data, use it for آغاز values
        elif not prev_data.empty:
            first_row = prev_data.iloc[0]
            form_data['alaqajat_start'] = int(first_row.get('alaqajat_ikhtitaam', 0))
            form_data['halqajat_start'] = int(first_row.get('halqajat_ikhtitaam', 0))
            form_data['halqajat_ward_start'] = int(first_row.get('halqajat_ward_ikhtitaam', 0))
            form_data['block_code_start'] = int(first_row.get('block_code_ikhtitaam', 0))
            form_data['arkaan_start'] = int(first_row.get('arkaan_ikhtitaam', 0))
            form_data['umeedwaran_start'] = int(first_row.get('umeedwaran_ikhtitaam', 0))
            form_data['hangami_start'] = int(first_row.get('hangami_ikhtitaam', 0))
            form_data['muawanin_start'] = int(first_row.get('muawanin_ikhtitaam', 0))
            form_data['mutayyin_afrad_start'] = int(first_row.get('mutayyin_afrad_ikhtitaam', 0))
            form_data['member_start'] = int(first_row.get('member_ikhtitaam', 0))
            # ... (set other fields as needed)
        else:
            # No data found, use default values
            print(f"No previous data found for {zila}, using default values")
            form_data.update({
                'union_committee_count': 0, 'wards_count': 0, 'block_code_count': 0, 'cantonment_board_count': 0,
                **default_values
            })
        return form_data
    except Exception as e:
        print(f"Error getting form data: {e}")
        return {}

@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        users_df = load_users_from_db()
        print(users_df)
        username = request.form['username']
        password = request.form['password']
        user = users_df[(users_df['username'] == username) & (users_df['password'] == password)]

        if not user.empty:
            session['username'] = username
            session['zila'] = user.iloc[0]['zila']
            session['role'] = user.iloc[0]['role']
            return redirect(url_for('report'))
        else:
            flash("غلط یوزر نیم یا پاس ورڈ", "error")

    return render_template('login.html')

@app.route('/report', methods=['GET', 'POST'])
def report():
    if 'username' not in session:
        return redirect(url_for('login'))
    
    zila = session['zila']
    current_month = datetime.today().strftime('%Y-%m')
    current_month_urdu = datetime.today().strftime('%B %Y')

    # Reload report data from DB
    global report_df
    report_df = load_reports_from_db()
    
    # Check if current month report already exists for this zila
    existing_report = report_df[(report_df['zila'] == zila) & (report_df['month'] == current_month)]
    report_exists = not existing_report.empty

    # Get form data (reuse get_form_data, but update it to use DB)
    form_data = get_form_data(zila, current_month)

    if request.method == 'POST':
        try:
            print(f"=== FORM SUBMISSION DEBUG ===")
            print(f"User: {session['username']}")
            print(f"Zila: {zila}")
            print(f"Current month: {current_month}")
            print(f"Report exists: {report_exists}")
            
            # Create comprehensive data entry from form
            # Only update fields that are empty in the DB for the current month
            new_entry = {
                'zila': zila,
                'month': current_month,
                'timestamp': datetime.now(),
                'submitted_by': session['username'],
            }
            for field in request.form:
                # Use the DB value if it exists, otherwise use the submitted value
                db_value = form_data.get(field, '')
                submitted_value = request.form.get(field, '')
                # For numbers, treat empty string as missing
                if db_value == '' or db_value is None:
                    new_entry[field] = submitted_value
                else:
                    new_entry[field] = db_value
            # Also add any fields that are not in the form but are in form_data (to preserve them)
            for field in form_data:
                if field not in new_entry:
                    new_entry[field] = form_data[field]
            
            print(f"[DEBUG] New entry created with {len(new_entry)} fields")
            
            # Calculate and save ikhtitaam values
            def get_int(form, key):
                try:
                    return int(form.get(key, 0))
                except Exception:
                    return 0

            # Organizational Units
            new_entry['alaqajat_ikhtitaam'] = get_int(request.form, 'alaqajat_start') + get_int(request.form, 'alaqajat_izafa') - get_int(request.form, 'alaqajat_kami')
            new_entry['halqajat_ikhtitaam'] = get_int(request.form, 'halqajat_start') + get_int(request.form, 'halqajat_izafa') - get_int(request.form, 'halqajat_kami')
            new_entry['halqajat_ward_ikhtitaam'] = get_int(request.form, 'halqajat_ward_start') + get_int(request.form, 'halqajat_ward_izafa') - get_int(request.form, 'halqajat_ward_kami')
            new_entry['block_code_ikhtitaam'] = get_int(request.form, 'block_code_start') + get_int(request.form, 'block_code_izafa') - get_int(request.form, 'block_code_kami')

            # Personnel
            new_entry['arkaan_ikhtitaam'] = get_int(request.form, 'arkaan_start') + get_int(request.form, 'arkaan_izafa') - get_int(request.form, 'arkaan_kami')
            new_entry['umeedwaran_ikhtitaam'] = get_int(request.form, 'umeedwaran_start') + get_int(request.form, 'umeedwaran_izafa') - get_int(request.form, 'umeedwaran_kami')
            new_entry['hangami_ikhtitaam'] = get_int(request.form, 'hangami_start') + get_int(request.form, 'hangami_izafa') - get_int(request.form, 'hangami_kami')
            new_entry['muawanin_ikhtitaam'] = get_int(request.form, 'muawanin_start') + get_int(request.form, 'muawanin_izafa') - get_int(request.form, 'muawanin_kami')
            new_entry['mutayyin_afrad_ikhtitaam'] = get_int(request.form, 'mutayyin_afrad_start') + get_int(request.form, 'mutayyin_afrad_izafa') - get_int(request.form, 'mutayyin_afrad_kami')
            new_entry['member_ikhtitaam'] = get_int(request.form, 'member_start') + get_int(request.form, 'member_izafa') - get_int(request.form, 'member_kami')
            
            # Collect dynamic youth program fields
            youth_programs = []

            # Collect server-rendered fields (programat_*)
            count = int(request.form.get('youth_programat_count', 0))
            for i in range(1, count + 1):
                name = request.form.get(f'programat_{i}', '').strip()
                num = request.form.get(f'programat_count_{i}', '').strip()
                if name or num:
                    youth_programs.append({'name': name, 'count': num})

            # Collect dynamically added fields (nauiyat_*)
            for key in request.form:
                if key.startswith('nauiyat_') and not key.startswith('nauiyat_count_'):
                    idx = key.split('_')[1]
                    name = request.form.get(f'nauiyat_{idx}', '').strip()
                    count_val = request.form.get(f'nauiyat_count_{idx}', '').strip()
                    if name or count_val:
                        youth_programs.append({'name': name, 'count': count_val})

            # Store as JSON
            new_entry['youth_programs_json'] = json.dumps(youth_programs, ensure_ascii=False)

            # Remove all nauiyat_* and nauiyat_count_* keys from new_entry
            for key in list(new_entry.keys()):
                if key.startswith('nauiyat_') or key.startswith('programat_'):
                    del new_entry[key]
            
            # Save to DB
            if report_exists:
                success = update_report_in_db(zila, current_month, new_entry)
                print(f"[DEBUG] update_report_in_db returned: {success}")
                if success is None:
                    flash("رپورٹ کامیابی سے اپ ڈیٹ ہو گئی ہے!", "success")
                else:
                    flash("رپورٹ اپ ڈیٹ کرنے میں خرابی", "error")
            else:
                save_report_to_db(new_entry)
                flash("رپورٹ کامیابی سے جمع ہو گئی ہے!", "success")
            
            print("=== END FORM SUBMISSION DEBUG ===")
            
        except Exception as e:
            flash(f"رپورٹ جمع کرنے میں خرابی: {str(e)}", "error")
            print(f"Error saving report: {e}")
            import traceback
            traceback.print_exc()

    return render_template("report.html", 
                         zila=zila, 
                         form_data=form_data,
                         report_exists=report_exists,
                         current_month=current_month_urdu)

@app.route('/dashboard')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("آپ کو ڈیش بورڈ دیکھنے کی اجازت نہیں ہے", "error")
        return redirect(url_for('report'))
    
    # Reload report data from DB
    global report_df
    report_df = load_reports_from_db()
    
    # Get summary statistics
    current_month = datetime.today().strftime('%Y-%m')
    current_reports = report_df[report_df['month'] == current_month]
    
    summary = {
        'total_zilas': len(current_reports),
        'total_arkaan': int(current_reports['halqajat_end'].sum()) if not current_reports.empty and 'halqajat_end' in current_reports.columns else 0,
        'total_members': int(current_reports['members_end'].sum()) if not current_reports.empty and 'members_end' in current_reports.columns else 0,
        'total_umeedwaran': int(current_reports['umeedwaran_end'].sum()) if not current_reports.empty and 'umeedwaran_end' in current_reports.columns else 0,
        'total_alaqajat': int(current_reports['alaqajat_end'].sum()) if not current_reports.empty and 'alaqajat_end' in current_reports.columns else 0,
        'total_halqajat': int(current_reports['halqajat_end'].sum()) if not current_reports.empty and 'halqajat_end' in current_reports.columns else 0,
    }
    
    return render_template("dashboard.html", 
                         summary=summary, 
                         reports=current_reports.to_dict())

@app.route('/logout')
def logout():
    session.clear()
    flash("آپ کامیابی سے لاگ آؤٹ ہو گئے ہیں", "info")
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)