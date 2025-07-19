from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from werkzeug.security import check_password_hash, generate_password_hash
import sqlite3
import logging




app = Flask(__name__)
app.secret_key = 'jamat-report987654'
application = app

# Use absolute path for DB file
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zila_data.db')

# Set up logging to a file
logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

def force_update_database_schema():
    """Force update database schema to ensure all new columns exist"""
    # First, ensure karkunan columns exist
    karkunan_columns = [
        'karkunan_start', 'karkunan_end', 'karkunan_target',
        'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam'
    ]
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get existing columns
    c.execute('PRAGMA table_info(monthly_reports)')
    existing_cols = [row[1] for row in c.fetchall()]
    
    # Add karkunan columns first
    for col in karkunan_columns:
        if col not in existing_cols:
            try:
                c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} INTEGER DEFAULT 0')
                print(f"Added karkunan column: {col}")
            except Exception as e:
                print(f"Error adding karkunan column {col}: {e}")
    
    conn.commit()
    conn.close()
    
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
        'ijtimai_tuaam_maqamat', 'ijtimai_tuaam_daurajat', 'ijtimai_tuaam_attendance',
        'ijtimai_ahle_khana_maqamat', 'ijtimai_ahle_khana_daurajat', 'ijtimai_ahle_khana_attendance',
        'quran_course_maqamat', 'quran_course_daurajat', 'quran_course_attendance',
        'retreat_maqamat', 'retreat_daurajat', 'retreat_attendance',
        'quran_courses', 'quran_classes', 'quran_participants',
        'fahem_quran_attendance', 'quran_target', 'quran_distributed',
        'central_training_target', 'central_training_actual', 'other_trainings',
        'atifal_programs', 'awaami_committees', 'awaami_committees_count',
        'koi_or_bat', 'haq_do_karachi',
        # Hangami/other workers
        'hangami_start', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam', 'hangami_end',
        # Members
        'member_start', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam', 'member_end',
        # Nizam e Fajar
        'nizam_e_fajar_start',  'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
        # Awaami Committee
        'awaami_committee_start',  'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
        # جماعتِ اسلامی شعبہ اطفال
        'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
    ]
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get existing columns
    c.execute('PRAGMA table_info(monthly_reports)')
    existing_cols = [row[1] for row in c.fetchall()]
    
    # Add missing columns
    for col in required_columns:
        if col not in existing_cols:
            try:
                c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} INTEGER DEFAULT 0')
                print(f"Added column: {col}")
            except Exception as e:
                print(f"Error adding column {col}: {e}")
    
    conn.commit()
    conn.close()
    print("Database schema update completed!")

def ensure_monthly_reports_columns():
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
        'ijtimai_tuaam_maqamat', 'ijtimai_tuaam_daurajat', 'ijtimai_tuaam_attendance',
        'ijtimai_ahle_khana_maqamat', 'ijtimai_ahle_khana_daurajat', 'ijtimai_ahle_khana_attendance',
        'quran_course_maqamat', 'quran_course_daurajat', 'quran_course_attendance',
        'retreat_maqamat', 'retreat_daurajat', 'retreat_attendance',
        'quran_courses', 'quran_classes', 'quran_participants',
        'fahem_quran_attendance', 'quran_target', 'quran_distributed',
        'central_training_target', 'central_training_actual', 'other_trainings',
        'atifal_programs', 'awaami_committees', 'awaami_committees_count',
        'koi_or_bat', 'haq_do_karachi',
        # Hangami/other workers
        'hangami_start', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam', 'hangami_end',
        # Members
        'member_start', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam', 'member_end',
        # Nizam e Fajar
        'nizam_e_fajar_start',  'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
        # Awaami Committee
        'awaami_committee_start',  'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
        # جماعتِ اسلامی شعبہ اطفال
        'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
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
            except Exception as e:
                pass
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
            last_submitted_by TEXT,
            union_committee_count INTEGER,
            wards_count INTEGER,
            block_code_count INTEGER,
            cantonment_board_count INTEGER,
            nazm_qaim_union INTEGER,
            nazm_qaim_wards INTEGER,
            nazm_qaim_blockcode INTEGER,
            nazm_qaim_cantonment INTEGER,
            UNIQUE(zila)
            -- Add all other fields here, matching the Google Sheets columns
        )
    ''')
    conn.commit()
    conn.close()
    ensure_monthly_reports_columns()

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
        return pd.DataFrame(rows, columns=pd.Index(['username', 'password', 'zila', 'role']))

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
            'جنوبی', 'کیماڑی', 'کورنگی', 'ملیر', 'قائدین',
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
    return pd.DataFrame(rows, columns=pd.Index(columns))

def save_report_to_db(new_entry):
    import logging
    try:
        logging.info(f"Attempting to save report: {new_entry}")
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        # Get actual columns in the table
        c.execute('PRAGMA table_info(monthly_reports)')
        table_columns = [row[1] for row in c.fetchall()]
        # Only keep keys that are real columns and filter out dynamic fields
        filtered_entry = {}
        for k, v in new_entry.items():
            if k in table_columns and not k.startswith('atifal_programat_') and not k.startswith('programat_'):
                filtered_entry[k] = v
        
        keys = list(filtered_entry.keys())
        values = [filtered_entry[k] for k in keys]
        placeholders = ','.join(['?'] * len(keys))
        sql = f"INSERT OR REPLACE INTO monthly_reports ({','.join(keys)}) VALUES ({placeholders})"
        c.execute(sql, values)
        conn.commit()
        c.execute('SELECT * FROM monthly_reports WHERE zila=? AND month=?', (filtered_entry['zila'], filtered_entry['month']))
        conn.close()
        logging.info("Report saved successfully.")
    except Exception as e:
        logging.error(f"Error in save_report_to_db: {e}", exc_info=True)
        raise

def update_report_in_db(zila, month, new_entry):
    import logging
    try:
        logging.info(f"Attempting to update report for zila={zila}: {new_entry}")
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        set_clause = ', '.join([f'{k}=?' for k in new_entry.keys()])
        values = list(new_entry.values())
        values.append(zila)
        sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=?"
        c.execute(sql, values)
        conn.commit()
        c.execute('SELECT * FROM monthly_reports WHERE zila=?', (zila,))
        conn.close()
        logging.info("Report updated successfully.")
    except Exception as e:
        logging.error(f"Error in update_report_in_db: {e}", exc_info=True)
        raise

# Load initial data
users_df = load_users_from_db()
report_df = load_reports_from_db()

def get_form_data(zila, current_month):
    """Get data for form fields from SQLite DB"""
    try:
        # Reload data from DB
        global report_df
        report_df = load_reports_from_db()
        # Get data for this zila (only one entry per zila)
        zila_data = report_df[report_df['zila'] == zila]
        form_data = {}
        # If we have data for this zila, use it
        if not zila_data.empty:
            first_row = zila_data.iloc[0]
            form_data = first_row.to_dict()
            # Convert NaN to empty string for all fields
            form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
            # Ensure youth_programat_count is present and string type
            if 'youth_programat_count' in form_data:
                form_data['youth_programat_count'] = str(form_data['youth_programat_count']) if form_data['youth_programat_count'] is not None else ''
            # Ensure atifal_programat_count is present and string type
            if 'atifal_programat_count' in form_data:
                form_data['atifal_programat_count'] = str(form_data['atifal_programat_count']) if form_data['atifal_programat_count'] is not None else ''
        else:
            # No data exists for this zila, start with empty form
            form_data = {}
            # Four basic info fields as empty string
            form_data['union_committee_count'] = ''
            form_data['wards_count'] = ''
            form_data['block_code_count'] = ''
            form_data['cantonment_board_count'] = ''
            # Also leave *_start and *_target fields empty
            for key in [
                'alaqajat', 'halqajat', 'halqajat_ward', 'block_code', 'nizam_e_fajar', 'awaami_committee',
                'arkaan', 'umeedwaran', 'karkunan', 'hangami', 'muawanin', 'mutayyin_afrad', 'member'
            ]:
                form_data[f'{key}_start'] = ''
                form_data[f'{key}_target'] = ''
        return form_data
    except Exception as e:
        import logging
        logging.error(f"Error getting form data: {e}", exc_info=True)
        return {}

@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        users_df = load_users_from_db()
        username = request.form['username']
        password = request.form['password']
        user = users_df[(users_df['username'] == username) & (users_df['password'] == password)]

        if not user.empty:
            session['username'] = username
            session['zila'] = user.iloc[0]['zila']
            session['role'] = user.iloc[0]['role']
            # Redirect admin to dashboard, others to report
            if user.iloc[0]['role'] == 'admin':
                return redirect(url_for('dashboard'))
            else:
                return redirect(url_for('report'))
        else:
            flash("غلط یوزر نیم یا پاس ورڈ", "error")

    return render_template('login.html')

@app.route('/report', methods=['GET', 'POST'])
def report():
    atifal_programs_list = []  # Always define this to avoid UnboundLocalError
    try:
        if 'username' not in session:
            return redirect(url_for('login'))
        zila = session['zila']
        current_month = datetime.today().strftime('%Y-%m')
        current_month_urdu = datetime.today().strftime('%B %Y')
        global report_df
        report_df = load_reports_from_db()
        existing_report = report_df[report_df['zila'] == zila]
        report_exists = not existing_report.empty
        form_data = get_form_data(zila, current_month)
        programat_list = []
        programat_json = form_data.get('youth_programs_json', '')
        if programat_json:
            try:
                programat_list = json.loads(programat_json)
            except Exception:
                programat_list = []
        visible_fields = [
            # ابتدائی معلومات
            'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
            'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
            # تنظیمی ہیٔت
            'alaqajat_target', 'alaqajat_start', 'alaqajat_izafa', 'alaqajat_kami', 'alaqajat_ikhtitaam',
            'halqajat_target', 'halqajat_start', 'halqajat_izafa', 'halqajat_kami', 'halqajat_ikhtitaam',
            'halqajat_ward_target', 'halqajat_ward_start', 'halqajat_ward_izafa', 'halqajat_ward_kami', 'halqajat_ward_ikhtitaam',
            'block_code_target', 'block_code_start', 'block_code_izafa', 'block_code_kami', 'block_code_ikhtitaam',
            'nizam_e_fajar_target', 'nizam_e_fajar_start', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
            'awaami_committee_target', 'awaami_committee_start', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
            # افرادی قوت
            'arkaan_target', 'arkaan_start', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
            'umeedwaran_target', 'umeedwaran_start', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
            'karkunan_target', 'karkunan_start', 'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam',
            'hangami_target', 'hangami_start', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
            'muawanin_target', 'muawanin_start', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
            'mutayyin_afrad_target', 'mutayyin_afrad_start', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
            'member_target', 'member_start', 'member_izafa', 'member_kami', 'member_ikhtitaam',
            # جماعت اسلامی یوتھ
            'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'youth_programs_json',
            # ضلعی تنظیمی اجتماعات
            'zilai_shura_planned', 'zilai_shura_held', 'zilai_shura_attendance',
            'nazm_zila_planned', 'nazm_zila_held', 'nazm_zila_attendance',
            'nazimin_alaqajat_planned', 'nazimin_alaqajat_held', 'nazimin_alaqajat_attendance',
            'zilai_ijtima_arkaan_planned', 'zilai_ijtima_arkaan_held', 'zilai_ijtima_arkaan_attendance',
            'zilai_ijtima_umeedwaran_planned', 'zilai_ijtima_umeedwaran_held', 'zilai_ijtima_umeedwaran_attendance',
            # تنظیمی اجتماعات
            'ijtima_arkaan_alaqah_planned', 'ijtima_arkaan_alaqah_held', 'ijtima_arkaan_alaqah_attendance',
            'ijtima_umeedwaran_alaqah_planned', 'ijtima_umeedwaran_alaqah_held', 'ijtima_umeedwaran_alaqah_attendance',
            'ijtima_karkunaan_alaqah_planned', 'ijtima_karkunaan_alaqah_held', 'ijtima_karkunaan_alaqah_attendance',
            'ijtima_karkunaan_halqajat_planned', 'ijtima_karkunaan_halqajat_held', 'ijtima_karkunaan_halqajat_attendance',
            'ijtima_nazimin_halqajat_planned', 'ijtima_nazimin_halqajat_held', 'ijtima_nazimin_halqajat_attendance',
            # دعوتی اجتماعات
            'dars_quran_planned', 'dars_quran_held', 'dars_quran_attendance',
            'dawati_camp_planned', 'dawati_camp_held', 'dawati_camp_attendance',
            'gharon_tak_dawat_planned', 'gharon_tak_dawat_held', 'gharon_tak_dawat_attendance',
            'taqseem_literature_planned', 'taqseem_literature_held', 'taqseem_literature_attendance',
            # تربیتی اجتماعات
            'study_circle_maqamat', 'study_circle_daurajat', 'study_circle_attendance',
            'ijtimai_tuaam_maqamat', 'ijtimai_tuaam_daurajat', 'ijtimai_tuaam_attendance',
            'ijtimai_ahle_khana_maqamat', 'ijtimai_ahle_khana_daurajat', 'ijtimai_ahle_khana_attendance',
            'quran_course_maqamat', 'quran_course_daurajat', 'quran_course_attendance',
            'retreat_maqamat', 'retreat_daurajat', 'retreat_attendance',
            # مرکز قرآن و سنہ
            'quran_courses', 'quran_classes', 'quran_participants',
            'fahem_quran_attendance', 'quran_distributed',
            # تنظیمی و دیگر سرگرمیاں
            'central_training_target', 'central_training_actual', 'other_trainings',
            'atifal_programs',
            # ذمہ داران
            'amir_zila_maqamat', 'amir_zila_daurajat', 'amir_zila_mulaqat',
            'qaim_zila_maqamat', 'qaim_zila_daurajat', 'qaim_zila_mulaqat',
            'naib_amir_zila_maqamat', 'naib_amir_zila_daurajat', 'naib_amir_zila_mulaqat',
            # کوئی اور بات
            'koi_or_bat', 'haq_do_karachi',
            # جماعتِ اسلامی شعبہ اطفال
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
        ]
        # --- NEW LOGIC: lock only filled fields, keep empty fields editable ---
        if request.method == 'POST':
            new_entry = dict(request.form)
            # Prevent admins from submitting atifal fields
            if session.get('role') == 'admin':
                for key in list(new_entry.keys()):
                    if key.startswith('atifal_'):
                        del new_entry[key]
            # Always set atifal_programat_count for both insert and update (like youth_programat_count)
            if 'atifal_programat_count' in new_entry:
                try:
                    new_entry['atifal_programat_count'] = str(int(new_entry['atifal_programat_count']))
                except Exception:
                    new_entry['atifal_programat_count'] = '0'
            # Collect all youth program fields from the form
            youth_programs = []
            i = 1
            while True:
                name = new_entry.get(f'programat_{i}')
                count = new_entry.get(f'programat_count_{i}')
                if name is None and count is None:
                    break
                if name or count:
                    youth_programs.append({'name': name or '', 'count': count or ''})
                i += 1
            new_entry['youth_programs_json'] = json.dumps(youth_programs, ensure_ascii=False)
            # Remove the dynamic youth fields from new_entry
            for key in list(new_entry.keys()):
                if key.startswith('programat_'):
                    del new_entry[key]
            # Collect dynamic atifal fields and store as JSON
            atifal_programs = []
            i = 1
            while True:
                nauiyat = new_entry.get(f'atifal_nauyiat_{i}')
                hazri = new_entry.get(f'atifal_programat_count_{i}')
                if nauiyat is None and hazri is None:
                    break
                if nauiyat or hazri:
                    atifal_programs.append({'nauiyat': nauiyat or '', 'hazri': hazri or ''})
                i += 1
            new_entry['atifal_programs_json'] = json.dumps(atifal_programs, ensure_ascii=False)
            # remove temporary keys
            for key in list(new_entry.keys()):
                if key.startswith('atifal_nauyiat_') or (key.startswith('atifal_programat_count_') and key != 'atifal_programat_count'):
                    del new_entry[key]
            new_entry['zila'] = zila
            new_entry['month'] = current_month
            new_entry['timestamp'] = datetime.now().isoformat()
            new_entry['submitted_by'] = session['username']
            new_entry['last_submitted_by'] = session.get('role', 'user')
            update_fields = {}
            if report_exists:
                row = existing_report.iloc[0]
                # Admin can always update pre-value fields
                if session.get('role') == 'admin':
                    admin_editable_fields = [
                        # Basic info
                        'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
                        # تنظیمی ہیٔت (target/start)
                        'alaqajat_target', 'alaqajat_start',
                        'halqajat_target', 'halqajat_start',
                        'halqajat_ward_target', 'halqajat_ward_start',
                        'block_code_target', 'block_code_start',
                        'nizam_e_fajar_target', 'nizam_e_fajar_start',
                        'awaami_committee_target', 'awaami_committee_start',
                        # افرادی قوت (target/start)
                        'arkaan_target', 'arkaan_start',
                        'umeedwaran_target', 'umeedwaran_start',
                        'hangami_target', 'hangami_start',
                        'muawanin_target', 'muawanin_start',
                        'mutayyin_afrad_target', 'mutayyin_afrad_start',
                        'member_target', 'member_start'
                    ]
                    for key in admin_editable_fields:
                        if key in new_entry:
                            update_fields[key] = new_entry[key]
                            # User logic: only update fields that are empty in DB and non-empty in form
            for key in visible_fields:
                if session.get('role') != 'admin':
                    form_value = new_entry.get(key, '').strip() if isinstance(new_entry.get(key, ''), str) else new_entry.get(key, '')
                    # Ensure row is defined before using it
                    if 'row' in locals() and row is not None:
                        db_value = str(row.get(key, '')).strip() if row.get(key, '') is not None else ''
                    else:
                        db_value = ''
                    if db_value in ['', 'nan', 'None'] and form_value not in ['', 'nan', 'None']:
                        # For start fields, convert empty string to 0 for database storage
                        if key.endswith('_start') and form_value == '':
                            update_fields[key] = '0'
                        else:
                            update_fields[key] = form_value
                # Always update youth_programat_count for users if present
                if session.get('role') != 'admin' and 'youth_programat_count' in new_entry:
                    try:
                        update_fields['youth_programat_count'] = str(int(new_entry['youth_programat_count']))
                    except Exception:
                        update_fields['youth_programat_count'] = '0'
                # Always update atifal_programat_count for users if present (like youth_programat_count)
                if session.get('role') != 'admin' and 'atifal_programat_count' in new_entry:
                    try:
                        update_fields['atifal_programat_count'] = str(int(new_entry['atifal_programat_count']))
                    except Exception:
                        update_fields['atifal_programat_count'] = '0'
                # Always update *_ikhtitaam fields if present
                ikhtitaam_fields = [
                    'alaqajat_ikhtitaam', 'halqajat_ikhtitaam', 'halqajat_ward_ikhtitaam', 'block_code_ikhtitaam',
                    'nizam_e_fajar_ikhtitaam', 'awaami_committee_ikhtitaam',
                    'arkaan_ikhtitaam', 'umeedwaran_ikhtitaam', 'hangami_ikhtitaam', 'muawanin_ikhtitaam',
                    'mutayyin_afrad_ikhtitaam', 'member_ikhtitaam'
                ]
                for key in ikhtitaam_fields:
                    if key in new_entry:
                        update_fields[key] = new_entry[key]
                update_fields['timestamp'] = new_entry['timestamp']
                update_fields['submitted_by'] = new_entry['submitted_by']
                update_fields['last_submitted_by'] = new_entry['last_submitted_by']
                if update_fields:
                    update_report_in_db(zila, current_month, update_fields)
                    flash("Updated successfully!", "success")
                else:
                    flash("No new fields to update.", "info")
            else:
                # Always ensure atifal_programat_count is included if present
                if 'atifal_programat_count' in new_entry:
                    try:
                        new_entry['atifal_programat_count'] = str(int(new_entry['atifal_programat_count']))
                    except Exception:
                        new_entry['atifal_programat_count'] = '0'
                # Convert empty start fields to 0 for database storage
                for key in list(new_entry.keys()):
                    if key.endswith('_start') and new_entry[key] == '':
                        new_entry[key] = '0'
                # Check if report already exists for this zila
                existing_report = report_df[report_df['zila'] == zila]
                if not existing_report.empty:
                    # Update existing report
                    update_report_in_db(zila, current_month, new_entry)
                    flash("Report updated successfully!", "success")
                else:
                    # Create new report
                    save_report_to_db(new_entry)
                    flash("Report submitted!", "success")
            return redirect(url_for('report'))
        # For GET: lock fields that are non-empty in DB, keep empty fields editable
        locked_fields = {}
        can_submit = True  # Always set a default value
        if report_exists:
            row = existing_report.iloc[0]
            # Ensure atifal_programat_count is in visible_fields
            if 'atifal_programat_count' not in visible_fields:
                visible_fields.append('atifal_programat_count')
            # Locking logic for all visible fields
            for key in visible_fields:
                locked_fields[key] = str(row.get(key, '')).strip() not in ['', 'nan', 'None']
            # Explicitly ensure atifal_programat_count is locked if filled
            value = row.get('atifal_programat_count', '')
            locked_fields['atifal_programat_count'] = value not in ['', 'nan', 'None', None]
            # --- Handle dynamic youth program fields for locking ---
        form_data = get_form_data(zila, current_month)
        programat_list = []
        programat_json = form_data.get('youth_programs_json', '')
        if programat_json:
            try:
                programat_list = json.loads(programat_json)
            except Exception:
                programat_list = []
            for idx, prog in enumerate(programat_list):
                locked_fields[f'programat_{idx+1}'] = bool(prog.get('name', '').strip())
                locked_fields[f'programat_count_{idx+1}'] = bool(str(prog.get('count', '')).strip())
            for idx, prog in enumerate(programat_list):
                form_data[f'programat_{idx+1}'] = prog.get('name', '')
                form_data[f'programat_count_{idx+1}'] = prog.get('count', '')
            # --- Handle dynamic atifal program fields for locking ---
            atifal_programs_list = []
            atifal_programs_json = form_data.get('atifal_programs_json', '')
            if atifal_programs_json:
                try:
                    atifal_programs_list = json.loads(atifal_programs_json)
                except Exception:
                    atifal_programs_list = []
            for idx, prog in enumerate(atifal_programs_list):
                locked_fields[f'atifal_nauyiat_{idx+1}'] = bool(prog.get('nauiyat', '').strip())
                locked_fields[f'atifal_programat_count_{idx+1}'] = bool(str(prog.get('hazri', '')).strip())
            for idx, prog in enumerate(atifal_programs_list):
                form_data[f'atifal_nauyiat_{idx+1}'] = prog.get('nauiyat', '')
                form_data[f'atifal_programat_count_{idx+1}'] = prog.get('hazri', '')
            can_submit = any(not locked_fields.get(k, False) for k in visible_fields) or any(
                not locked_fields.get(f'programat_{i+1}', False) or not locked_fields.get(f'programat_count_{i+1}', False)
                for i in range(len(programat_list))
            ) or any(
                not locked_fields.get(f'atifal_nauyiat_{i+1}', False) or not locked_fields.get(f'atifal_programat_count_{i+1}', False)
                for i in range(len(atifal_programs_list))
            )
        else:
            for key in visible_fields:
                locked_fields[key] = False
            can_submit = True
        # All assignments to form_data are done above
        # Now populate atifal fields for template compatibility
        atifal_programs_list = []
        atifal_programs_json = form_data.get('atifal_programs_json', '')
        if atifal_programs_json:
            try:
                atifal_programs_list = json.loads(atifal_programs_json)
            except Exception:
                atifal_programs_list = []
        # Populate form_data with atifal program fields for template compatibility
        for idx, prog in enumerate(atifal_programs_list):
            form_data[f'atifal_nauyiat_{idx+1}'] = prog.get('nauiyat', '')
            form_data[f'atifal_programat_count_{idx+1}'] = prog.get('hazri', '')
            # Lock fields if they have data
            if prog.get('nauiyat', '').strip() or prog.get('hazri', '').strip():
                locked_fields[f'atifal_nauyiat_{idx+1}'] = True
                locked_fields[f'atifal_programat_count_{idx+1}'] = True
        return render_template("report.html", 
                             zila=zila, 
                             form_data=form_data,
                             report_exists=report_exists,
                             current_month=current_month_urdu,
                             programat_list=programat_list,
                             locked_fields=locked_fields,
                             can_submit=can_submit)
    except Exception as e:
        import logging
        logging.error("Exception in /report route", exc_info=True)
        return "Internal Server Error. Please check error.log for details.", 500

@app.route('/dashboard')
def dashboard():
    try:
        if 'username' not in session:
            return redirect(url_for('login'))
        if session.get('role') != 'admin':
            flash("آپ کو ڈیش بورڈ دیکھنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('report'))
        global report_df, users_df
        report_df = load_reports_from_db()
        users_df = load_users_from_db()
        current_month = datetime.today().strftime('%Y-%m')
        current_reports = report_df[report_df['month'] == current_month]
        all_zilas = list(pd.Series(users_df[users_df['role'] == 'user']['zila']).drop_duplicates())
        zila_status = []
        org_keys = [
            'alaqajat', 'halqajat', 'halqajat_ward', 'block_code', 'nizam_e_fajar', 'awaami_committee'
        ]
        manpower_keys = [
            'arkaan', 'umeedwaran', 'karkunan', 'hangami', 'muawanin', 'mutayyin_afrad', 'member'
        ]
        visible_fields = [
            'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
            'alaqajat_izafa', 'alaqajat_kami', 'halqajat_izafa', 'halqajat_kami',
            'halqajat_ward_izafa', 'halqajat_ward_kami', 'block_code_izafa', 'block_code_kami',
            'arkaan_izafa', 'arkaan_kami', 'umeedwaran_izafa', 'umeedwaran_kami',
            'karkunan_izafa', 'karkunan_kami',
            'hangami_izafa', 'hangami_kami', 'muawanin_izafa', 'muawanin_kami',
            'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'member_izafa', 'member_kami',
            'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'awaami_committee_izafa', 'awaami_committee_kami',
            'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'koi_or_bat', 'haq_do_karachi',
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count',
            'ijtimai_tuaam_maqamat', 'ijtimai_tuaam_daurajat', 'ijtimai_tuaam_attendance',
            'ijtimai_ahle_khana_maqamat', 'ijtimai_ahle_khana_daurajat', 'ijtimai_ahle_khana_attendance',
        ]
        for zila in all_zilas:
            report_row = pd.DataFrame(current_reports[current_reports['zila'] == zila])
            zila_dict = {
                'zila': zila,
                'union_committee_count': '',
                'wards_count': '',
                'block_code_count': '',
                'cantonment_board_count': '',
            }
            for key in org_keys:
                zila_dict[f'{key}_target'] = ''
                zila_dict[f'{key}_start'] = ''
            for key in manpower_keys:
                zila_dict[f'{key}_target'] = ''
                zila_dict[f'{key}_start'] = ''
            if not report_row.empty:
                row = report_row.iloc[0]
                zila_dict['union_committee_count'] = row.get('union_committee_count', '')
                zila_dict['wards_count'] = row.get('wards_count', '')
                zila_dict['block_code_count'] = row.get('block_code_count', '')
                zila_dict['cantonment_board_count'] = row.get('cantonment_board_count', '')
                for key in org_keys:
                    zila_dict[f'{key}_target'] = row.get(f'{key}_target', '')
                    zila_dict[f'{key}_start'] = row.get(f'{key}_start', '')
                for key in manpower_keys:
                    zila_dict[f'{key}_target'] = row.get(f'{key}_target', '')
                    zila_dict[f'{key}_start'] = row.get(f'{key}_start', '')
                # Check completeness of required fields
                total_fields = len(visible_fields)
                filled_fields = sum(1 for field in visible_fields if str(row.get(field, '')).strip() not in ['', 'nan', 'None'])
                if filled_fields == 0:
                    zila_dict['status'] = 'not_started'
                elif filled_fields == total_fields:
                    zila_dict['status'] = 'submitted'
                else:
                    zila_dict['status'] = 'partial'
                zila_dict['has_report'] = True
            else:
                zila_dict['status'] = 'not_started'
                zila_dict['has_report'] = False
            zila_status.append(zila_dict)
        summary = {
            'total_zilas': len(all_zilas),
            'total_submitted_reports': 0,
            'total_not_submitted_reports': 0,
            'total_arkaan': 0,
            'total_members': 0,
            'total_collections': 0,
        }
        reports_list = []
        return render_template("dashboard.html", 
                             summary=summary, 
                             reports=reports_list,
                             zila_status=zila_status,
                             current_month=current_month)
    except Exception as e:
        import logging
        logging.error("Exception in /dashboard route", exc_info=True)
        return "Internal Server Error. Please check error.log for details.", 500

@app.route('/update_zila_info', methods=['POST'])
def update_zila_info():
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash('صرف ایڈمن اس صفحہ تک رسائی حاصل کر سکتا ہے۔')
            return redirect(url_for('dashboard'))
        from datetime import datetime
        current_month = datetime.today().strftime('%Y-%m')
        # Get all zila keys from the form
        zila_keys = [k for k in request.form.keys() if k.startswith('zila_')]
        for key in zila_keys:
            idx = key.split('_')[1]
            zila = request.form.get(f'zila_{idx}')
            ibtidai_maloomat_tadad = request.form.get(f'ibtidai_maloomat_tadad_{idx}') or None
            tanzeemi_hayat = request.form.get(f'tanzeemi_hayat_{idx}') or None
            afradi_quwat = request.form.get(f'afradi_quwat_{idx}') or None
            # Only update if at least one value is provided
            update_fields = {}
            if ibtidai_maloomat_tadad is not None:
                update_fields['ibtidai_maloomat_tadad'] = ibtidai_maloomat_tadad
            if tanzeemi_hayat is not None:
                update_fields['tanzeemi_hayat'] = tanzeemi_hayat
            if afradi_quwat is not None:
                update_fields['afradi_quwat'] = afradi_quwat
            if update_fields:
                # Check if report exists for this zila and month
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute('SELECT id FROM monthly_reports WHERE zila=? AND month=?', (zila, current_month))
                row = c.fetchone()
                if row:
                    # Update existing
                    set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
                    values = list(update_fields.values())
                    values.extend([zila, current_month])
                    sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND month=?"
                    c.execute(sql, values)
                else:
                    # Insert new
                    sql = "INSERT INTO monthly_reports (zila, month, ibtidai_maloomat_tadad, tanzeemi_hayat, afradi_quwat, timestamp, submitted_by) VALUES (?, ?, ?, ?, ?, datetime('now'), ?)"
                    c.execute(sql, (zila, current_month, ibtidai_maloomat_tadad, tanzeemi_hayat, afradi_quwat, session.get('username', 'admin')))
                conn.commit()
                conn.close()
        flash('معلومات کامیابی سے محفوظ ہو گئیں۔')
        return redirect(url_for('dashboard'))
    except Exception as e:
        import logging
        logging.error("Exception in /update_zila_info route", exc_info=True)
        flash('محفوظ کرنے میں خرابی۔')
        return redirect(url_for('dashboard'))

@app.route('/update_basic_info', methods=['POST'])
def update_basic_info():
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash('صرف ایڈمن اس صفحہ تک رسائی حاصل کر سکتا ہے۔')
            return redirect(url_for('dashboard'))
        from datetime import datetime
        current_month = datetime.today().strftime('%Y-%m')
        zila_keys = [k for k in request.form.keys() if k.startswith('zila_')]
        for key in zila_keys:
            idx = key.split('_')[1]
            zila = request.form.get(f'zila_{idx}')
            union_committee_count = request.form.get(f'union_committee_count_{idx}') or None
            wards_count = request.form.get(f'wards_count_{idx}') or None
            block_code_count = request.form.get(f'block_code_count_{idx}') or None
            cantonment_board_count = request.form.get(f'cantonment_board_count_{idx}') or None
            update_fields = {
                'union_committee_count': union_committee_count,
                'wards_count': wards_count,
                'block_code_count': block_code_count,
                'cantonment_board_count': cantonment_board_count
            }
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute('SELECT id FROM monthly_reports WHERE zila=? AND month=?', (zila, current_month))
            row = c.fetchone()
            if row:
                set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
                values = list(update_fields.values())
                values.extend([zila, current_month])
                sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND month=?"
                c.execute(sql, values)
            else:
                sql = "INSERT INTO monthly_reports (zila, month, union_committee_count, wards_count, block_code_count, cantonment_board_count, timestamp, submitted_by) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)"
                c.execute(sql, (zila, current_month, union_committee_count, wards_count, block_code_count, cantonment_board_count, session.get('username', 'admin')))
            conn.commit()
            conn.close()
        flash('ابتدائی معلومات کامیابی سے محفوظ ہو گئیں۔')
        return redirect(url_for('dashboard'))
    except Exception as e:
        import logging
        logging.error("Exception in /update_basic_info route", exc_info=True)
        flash('محفوظ کرنے میں خرابی۔')
        return redirect(url_for('dashboard'))

@app.route('/update_org_structure', methods=['POST'])
def update_org_structure():
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash('صرف ایڈمن اس صفحہ تک رسائی حاصل کر سکتا ہے۔')
            return redirect(url_for('dashboard'))
        from datetime import datetime
        current_month = datetime.today().strftime('%Y-%m')
        zila = request.form.get('zila')
        org_keys = [
            'alaqajat', 'halqajat', 'halqajat_ward', 'block_code', 'nizam_e_fajar', 'awaami_committee'
        ]
        update_fields = {}
        for key in org_keys:
            update_fields[f'{key}_target'] = request.form.get(f'{key}_target') or None
            update_fields[f'{key}_start'] = request.form.get(f'{key}_start') or None
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('SELECT id FROM monthly_reports WHERE zila=? AND month=?', (zila, current_month))
        row = c.fetchone()
        if row:
            set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
            values = list(update_fields.values())
            values.extend([zila, current_month])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND month=?"
            c.execute(sql, values)
        else:
            sql = f"INSERT INTO monthly_reports (zila, month, {', '.join(update_fields.keys())}, timestamp, submitted_by) VALUES (?, ?, {', '.join(['?']*len(update_fields))}, datetime('now'), ?)"
            c.execute(sql, (zila, current_month, *update_fields.values(), session.get('username', 'admin')))
        conn.commit()
        conn.close()
        flash('تنظیمی ہیٔت کامیابی سے محفوظ ہو گئیں۔')
        return redirect(url_for('dashboard'))
    except Exception as e:
        import logging
        logging.error("Exception in /update_org_structure route", exc_info=True)
        flash('محفوظ کرنے میں خرابی۔')
        return redirect(url_for('dashboard'))

@app.route('/update_manpower', methods=['POST'])
def update_manpower():
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash('صرف ایڈمن اس صفحہ تک رسائی حاصل کر سکتا ہے۔')
            return redirect(url_for('dashboard'))
        from datetime import datetime
        current_month = datetime.today().strftime('%Y-%m')
        zila = request.form.get('zila')
        manpower_keys = [
            'arkaan', 'umeedwaran', 'karkunan', 'hangami', 'muawanin', 'mutayyin_afrad', 'member'
        ]
        update_fields = {}
        for key in manpower_keys:
            update_fields[f'{key}_target'] = request.form.get(f'{key}_target') or None
            update_fields[f'{key}_start'] = request.form.get(f'{key}_start') or None
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('SELECT id FROM monthly_reports WHERE zila=? AND month=?', (zila, current_month))
        row = c.fetchone()
        if row:
            set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
            values = list(update_fields.values())
            values.extend([zila, current_month])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND month=?"
            c.execute(sql, values)
        else:
            sql = f"INSERT INTO monthly_reports (zila, month, {', '.join(update_fields.keys())}, timestamp, submitted_by) VALUES (?, ?, {', '.join(['?']*len(update_fields))}, datetime('now'), ?)"
            c.execute(sql, (zila, current_month, *update_fields.values(), session.get('username', 'admin')))
        conn.commit()
        conn.close()
        flash('افرادی قوت کامیابی سے محفوظ ہو گئیں۔')
        return redirect(url_for('dashboard'))
    except Exception as e:
        import logging
        logging.error("Exception in /update_manpower route", exc_info=True)
        flash('محفوظ کرنے میں خرابی۔')
        return redirect(url_for('dashboard'))

@app.route('/view_report/<zila>/<month>')
def view_report(zila, month):
    try:
        if 'username' not in session or session.get('role') != 'admin':
            return redirect(url_for('login'))
        global report_df
        report_df = load_reports_from_db()
        report_row = report_df[(report_df['zila'] == zila) & (report_df['month'] == month)]
        if report_row.empty:
            flash("اس ضلع کی رپورٹ موجود نہیں ہے۔", "error")
            return redirect(url_for('dashboard'))
        form_data = report_row.iloc[0].to_dict()
        # Convert NaN to empty string
        form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
        # For display, get month in Urdu
        current_month_urdu = datetime.strptime(month, '%Y-%m').strftime('%B %Y')
        # All fields locked (read-only)
        locked_fields = {k: True for k in form_data.keys()}
        programat_list = []
        programat_json = form_data.get('youth_programs_json', '')
        if programat_json:
            try:
                programat_list = json.loads(programat_json)
            except Exception:
                programat_list = []
        # Load atifal programs list from JSON
        atifal_programs_list = []
        atifal_programs_json = form_data.get('atifal_programs_json', '')
        if atifal_programs_json:
            try:
                atifal_programs_list = json.loads(atifal_programs_json)
            except Exception:
                atifal_programs_list = []
        # Populate form_data fields for template compatibility
        for idx, prog in enumerate(atifal_programs_list):
            form_data[f'atifal_nauyiat_{idx+1}'] = prog.get('nauiyat', '')
            form_data[f'atifal_programat_count_{idx+1}'] = prog.get('hazri', '')
        return render_template("report.html", 
                             zila=zila, 
                             form_data=form_data,
                             report_exists=True,
                             current_month=current_month_urdu,
                             programat_list=programat_list,
                             atifal_programs_list=atifal_programs_list,
                             locked_fields=locked_fields,
                             can_submit=False)
    except Exception as e:
        import logging
        logging.error("Exception in /view_report route", exc_info=True)
        flash("رپورٹ دیکھنے میں خرابی۔", "error")
        return redirect(url_for('dashboard'))

@app.route('/set_zila_and_redirect', methods=['POST'])
def set_zila_and_redirect():
    if 'username' not in session or session.get('role') != 'admin':
        return redirect(url_for('login'))
    zila = request.form.get('zila')
    if zila:
        session['zila'] = zila
    return redirect(url_for('report'))

@app.route('/update_schema')
def update_schema():
    """Route to manually update database schema"""
    try:
        force_update_database_schema()
        return "Database schema updated successfully!", 200
    except Exception as e:
        return f"Error updating schema: {str(e)}", 500

@app.route('/fix_karkunan_columns')
def fix_karkunan_columns():
    """Route to specifically fix karkunan columns"""
    try:
        karkunan_columns = [
            'karkunan_start', 'karkunan_end', 'karkunan_target',
            'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam'
        ]
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get existing columns
        c.execute('PRAGMA table_info(monthly_reports)')
        existing_cols = [row[1] for row in c.fetchall()]
        
        # Add missing karkunan columns
        added_columns = []
        for col in karkunan_columns:
            if col not in existing_cols:
                try:
                    c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} INTEGER DEFAULT 0')
                    added_columns.append(col)
                except Exception as e:
                    return f"Error adding column {col}: {str(e)}", 500
        
        conn.commit()
        conn.close()
        
        if added_columns:
            return f"Successfully added karkunan columns: {', '.join(added_columns)}", 200
        else:
            return "All karkunan columns already exist!", 200
            
    except Exception as e:
        return f"Error fixing karkunan columns: {str(e)}", 500

@app.route('/fix_youth_atifal_columns')
def fix_youth_atifal_columns():
    """Route to specifically fix youth and atifal program columns"""
    try:
        missing_columns = [
            'youth_programs_json', 'atifal_programs_json'
        ]
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get existing columns
        c.execute('PRAGMA table_info(monthly_reports)')
        existing_cols = [row[1] for row in c.fetchall()]
        
        # Add missing columns
        added_columns = []
        for col in missing_columns:
            if col not in existing_cols:
                try:
                    c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} TEXT')
                    added_columns.append(col)
                except Exception as e:
                    return f"Error adding column {col}: {str(e)}", 500
        
        conn.commit()
        conn.close()
        
        if added_columns:
            return f"Successfully added youth/atifal columns: {', '.join(added_columns)}", 200
        else:
            return "All youth/atifal columns already exist!", 200
            
    except Exception as e:
        return f"Error fixing youth/atifal columns: {str(e)}", 500

@app.route('/cleanup_duplicates')
def cleanup_duplicates():
    """Route to clean up duplicate entries in the database"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Find duplicates by zila only
        c.execute('''
            SELECT zila, COUNT(*) as count
            FROM monthly_reports 
            GROUP BY zila 
            HAVING COUNT(*) > 1
        ''')
        duplicates = c.fetchall()
        
        cleaned_count = 0
        for zila, count in duplicates:
            # Keep the most recent entry (highest id) and delete others
            c.execute('''
                DELETE FROM monthly_reports 
                WHERE zila = ? AND id NOT IN (
                    SELECT MAX(id) FROM monthly_reports 
                    WHERE zila = ?
                )
            ''', (zila, zila))
            cleaned_count += count - 1
        
        conn.commit()
        conn.close()
        
        return f"Cleaned up {cleaned_count} duplicate entries!", 200
            
    except Exception as e:
        return f"Error cleaning up duplicates: {str(e)}", 500

@app.route('/logout')
def logout():
    session.clear()
    flash("آپ کامیابی سے لاگ آؤٹ ہو گئے ہیں", "info")
    return redirect(url_for('login'))

@app.route('/print_schema')
def print_schema():
    import sqlite3
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('PRAGMA table_info(monthly_reports)')
    columns = c.fetchall()
    conn.close()
    return '<br>'.join([str(col) for col in columns])

@app.route('/fix_atifal_column')
def fix_atifal_column():
    import sqlite3
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("PRAGMA table_info(monthly_reports)")
        columns = [row[1] for row in c.fetchall()]
        if 'atifal_programat_count' not in columns:
            c.execute("ALTER TABLE monthly_reports ADD COLUMN atifal_programat_count INTEGER")
            conn.commit()
            msg = 'Column atifal_programat_count added.'
        else:
            msg = 'Column atifal_programat_count already exists.'
        conn.close()
        return msg
    except Exception as e:
        return f'Error: {e}'

@app.route('/debug_atifal_data')
def debug_atifal_data():
    """Route to debug atifal data in the database"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get all atifal data
        c.execute('SELECT zila, atifal_programat_count, atifal_programs_json FROM monthly_reports WHERE atifal_programs_json IS NOT NULL AND atifal_programs_json != ""')
        rows = c.fetchall()
        
        debug_info = []
        for zila, count, json_data in rows:
            try:
                programs = json.loads(json_data) if json_data else []
                debug_info.append(f"Zila: {zila}, Count: {count}, Programs: {programs}")
            except Exception as e:
                debug_info.append(f"Zila: {zila}, Count: {count}, JSON Error: {e}")
        
        conn.close()
        
        if debug_info:
            return "<br>".join(debug_info)
        else:
            return "No atifal data found in database"
            
    except Exception as e:
        return f"Error debugging atifal data: {str(e)}"

if __name__ == "__main__":
    init_db()
    force_update_database_schema()  # Force update schema to ensure all new columns exist
    app.run(debug=True, host='0.0.0.0', port=5001)