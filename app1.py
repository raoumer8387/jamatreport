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
            except Exception as e:
                pass
    
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
        # Historical data columns
        'period', 'submitted_by',
    ]
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get existing columns
    c.execute('PRAGMA table_info(monthly_reports)')
    existing_cols = [row[1] for row in c.fetchall()]
    
    # Define text columns that should be TEXT type
    text_columns = {'youth_activities', 'period', 'submitted_by', 'koi_or_bat', 'haq_do_karachi', 'other_trainings'}
    
    # Add missing columns
    for col in required_columns:
        if col not in existing_cols:
            try:
                col_type = 'TEXT' if col in text_columns else 'INTEGER DEFAULT 0'
                c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} {col_type}')
            except Exception as e:
                pass
    
    conn.commit()
    conn.close()

def ensure_monthly_reports_columns():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get current columns
    c.execute("PRAGMA table_info(monthly_reports)")
    existing_columns = [column[1] for column in c.fetchall()]
    
    # Add missing columns if they don't exist
    columns_to_add = [
        ('period', 'TEXT'),
        ('alaqajat_start', 'INTEGER'),
        ('alaqajat_end', 'INTEGER'),
        ('alaqajat_target', 'INTEGER'),
        ('alaqajat_izafa', 'INTEGER'),
        ('alaqajat_kami', 'INTEGER'),
        ('alaqajat_ikhtitaam', 'INTEGER'),
        ('halqajat_start', 'INTEGER'),
        ('halqajat_end', 'INTEGER'),
        ('halqajat_target', 'INTEGER'),
        ('halqajat_izafa', 'INTEGER'),
        ('halqajat_kami', 'INTEGER'),
        ('halqajat_ikhtitaam', 'INTEGER'),
        ('halqajat_ward_start', 'INTEGER'),
        ('halqajat_ward_end', 'INTEGER'),
        ('halqajat_ward_target', 'INTEGER'),
        ('halqajat_ward_izafa', 'INTEGER'),
        ('halqajat_ward_kami', 'INTEGER'),
        ('halqajat_ward_ikhtitaam', 'INTEGER'),
        ('block_code_start', 'INTEGER'),
        ('block_code_end', 'INTEGER'),
        ('block_code_target', 'INTEGER'),
        ('block_code_izafa', 'INTEGER'),
        ('block_code_kami', 'INTEGER'),
        ('block_code_ikhtitaam', 'INTEGER'),
        ('arkaan_start', 'INTEGER'),
        ('arkaan_end', 'INTEGER'),
        ('arkaan_target', 'INTEGER'),
        ('arkaan_izafa', 'INTEGER'),
        ('arkaan_kami', 'INTEGER'),
        ('arkaan_ikhtitaam', 'INTEGER'),
        ('umeedwaran_start', 'INTEGER'),
        ('umeedwaran_end', 'INTEGER'),
        ('umeedwaran_target', 'INTEGER'),
        ('umeedwaran_izafa', 'INTEGER'),
        ('umeedwaran_kami', 'INTEGER'),
        ('umeedwaran_ikhtitaam', 'INTEGER'),
        ('karkunan_start', 'INTEGER'),
        ('karkunan_end', 'INTEGER'),
        ('karkunan_target', 'INTEGER'),
        ('karkunan_izafa', 'INTEGER'),
        ('karkunan_kami', 'INTEGER'),
        ('karkunan_ikhtitaam', 'INTEGER'),
        ('hangami_start', 'INTEGER'),
        ('hangami_end', 'INTEGER'),
        ('hangami_target', 'INTEGER'),
        ('hangami_izafa', 'INTEGER'),
        ('hangami_kami', 'INTEGER'),
        ('hangami_ikhtitaam', 'INTEGER'),
        ('muawanin_start', 'INTEGER'),
        ('muawanin_end', 'INTEGER'),
        ('muawanin_target', 'INTEGER'),
        ('muawanin_izafa', 'INTEGER'),
        ('muawanin_kami', 'INTEGER'),
        ('muawanin_ikhtitaam', 'INTEGER'),
        ('mutayyin_afrad_start', 'INTEGER'),
        ('mutayyin_afrad_end', 'INTEGER'),
        ('mutayyin_afrad_target', 'INTEGER'),
        ('mutayyin_afrad_izafa', 'INTEGER'),
        ('mutayyin_afrad_kami', 'INTEGER'),
        ('mutayyin_afrad_ikhtitaam', 'INTEGER'),
        ('member_start', 'INTEGER'),
        ('member_end', 'INTEGER'),
        ('member_target', 'INTEGER'),
        ('member_izafa', 'INTEGER'),
        ('member_kami', 'INTEGER'),
        ('member_ikhtitaam', 'INTEGER'),
        ('youth_nazm_areas', 'INTEGER'),
        ('youth_karkunan', 'INTEGER'),
        ('youth_programat_count', 'INTEGER'),
        ('youth_programs_json', 'TEXT'),
        ('atifal_nazm_areas', 'INTEGER'),
        ('atifal_members', 'INTEGER'),
        ('atifal_programat_count', 'INTEGER'),
        ('atifal_programs_json', 'TEXT'),
        ('zilai_shura_planned', 'INTEGER'),
        ('zilai_shura_held', 'INTEGER'),
        ('zilai_shura_attendance', 'INTEGER'),
        ('nazm_zila_planned', 'INTEGER'),
        ('nazm_zila_held', 'INTEGER'),
        ('nazm_zila_attendance', 'INTEGER'),
        ('nazimin_alaqajat_planned', 'INTEGER'),
        ('nazimin_alaqajat_held', 'INTEGER'),
        ('nazimin_alaqajat_attendance', 'INTEGER'),
        ('zilai_ijtima_arkaan_planned', 'INTEGER'),
        ('zilai_ijtima_arkaan_held', 'INTEGER'),
        ('zilai_ijtima_arkaan_attendance', 'INTEGER'),
        ('zilai_ijtima_umeedwaran_planned', 'INTEGER'),
        ('zilai_ijtima_umeedwaran_held', 'INTEGER'),
        ('zilai_ijtima_umeedwaran_attendance', 'INTEGER'),
        ('ijtima_arkaan_alaqah_planned', 'INTEGER'),
        ('ijtima_arkaan_alaqah_held', 'INTEGER'),
        ('ijtima_arkaan_alaqah_attendance', 'INTEGER'),
        ('ijtima_umeedwaran_alaqah_planned', 'INTEGER'),
        ('ijtima_umeedwaran_alaqah_held', 'INTEGER'),
        ('ijtima_umeedwaran_alaqah_attendance', 'INTEGER'),
        ('ijtima_karkunaan_alaqah_planned', 'INTEGER'),
        ('ijtima_karkunaan_alaqah_held', 'INTEGER'),
        ('ijtima_karkunaan_alaqah_attendance', 'INTEGER'),
        ('ijtima_karkunaan_halqajat_planned', 'INTEGER'),
        ('ijtima_karkunaan_halqajat_held', 'INTEGER'),
        ('ijtima_karkunaan_halqajat_attendance', 'INTEGER'),
        ('ijtima_nazimin_halqajat_planned', 'INTEGER'),
        ('ijtima_nazimin_halqajat_held', 'INTEGER'),
        ('ijtima_nazimin_halqajat_attendance', 'INTEGER'),
        ('dars_quran_planned', 'INTEGER'),
        ('dars_quran_held', 'INTEGER'),
        ('dars_quran_attendance', 'INTEGER'),
        ('dawati_camp_planned', 'INTEGER'),
        ('dawati_camp_held', 'INTEGER'),
        ('dawati_camp_attendance', 'INTEGER'),
        ('gharon_tak_dawat_planned', 'INTEGER'),
        ('gharon_tak_dawat_held', 'INTEGER'),
        ('gharon_tak_dawat_attendance', 'INTEGER'),
        ('taqseem_literature_planned', 'INTEGER'),
        ('taqseem_literature_held', 'INTEGER'),
        ('taqseem_literature_attendance', 'INTEGER'),
        ('study_circle_maqamat', 'INTEGER'),
        ('study_circle_daurajat', 'INTEGER'),
        ('study_circle_attendance', 'INTEGER'),
        ('ijtimai_tuaam_maqamat', 'INTEGER'),
        ('ijtimai_tuaam_daurajat', 'INTEGER'),
        ('ijtimai_tuaam_attendance', 'INTEGER'),
        ('ijtimai_ahle_khana_maqamat', 'INTEGER'),
        ('ijtimai_ahle_khana_daurajat', 'INTEGER'),
        ('ijtimai_ahle_khana_attendance', 'INTEGER'),
        ('quran_course_maqamat', 'INTEGER'),
        ('quran_course_daurajat', 'INTEGER'),
        ('quran_course_attendance', 'INTEGER'),
        ('retreat_maqamat', 'INTEGER'),
        ('retreat_daurajat', 'INTEGER'),
        ('retreat_attendance', 'INTEGER'),
        ('quran_courses', 'INTEGER'),
        ('quran_classes', 'INTEGER'),
        ('quran_participants', 'INTEGER'),
        ('fahem_quran_attendance', 'INTEGER'),
        ('quran_distributed', 'INTEGER'),
        ('central_training_target', 'INTEGER'),
        ('central_training_actual', 'INTEGER'),
        ('other_trainings', 'TEXT'),
        ('atifal_programs', 'INTEGER'),
        ('amir_zila_maqamat', 'INTEGER'),
        ('amir_zila_daurajat', 'INTEGER'),
        ('amir_zila_mulaqat', 'INTEGER'),
        ('qaim_zila_maqamat', 'INTEGER'),
        ('qaim_zila_daurajat', 'INTEGER'),
        ('qaim_zila_mulaqat', 'INTEGER'),
        ('naib_amir_zila_maqamat', 'INTEGER'),
        ('naib_amir_zila_daurajat', 'INTEGER'),
        ('naib_amir_zila_mulaqat', 'INTEGER'),
        ('koi_or_bat', 'TEXT'),
        ('haq_do_karachi', 'TEXT'),
        ('nizam_e_fajar_start', 'INTEGER'),
        ('nizam_e_fajar_target', 'INTEGER'),
        ('nizam_e_fajar_izafa', 'INTEGER'),
        ('nizam_e_fajar_kami', 'INTEGER'),
        ('nizam_e_fajar_ikhtitaam', 'INTEGER'),
        ('awaami_committee_start', 'INTEGER'),
        ('awaami_committee_target', 'INTEGER'),
        ('awaami_committee_izafa', 'INTEGER'),
        ('awaami_committee_kami', 'INTEGER'),
        ('awaami_committee_ikhtitaam', 'INTEGER'),
        ('is_locked', 'INTEGER DEFAULT 0')  # Add is_locked column with default value 0 (unlocked)
    ]
    
    for column_name, column_type in columns_to_add:
        if column_name not in existing_columns:
            try:
                c.execute(f"ALTER TABLE monthly_reports ADD COLUMN {column_name} {column_type}")
                print(f"Added column: {column_name}")
            except Exception as e:
                print(f"Error adding column {column_name}: {e}")
    
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
            'admin', 'admin2',
            'airport', 'gadap', 'gharbi', 'wasti', 'gulberg_wasti', 
            'junoobi', 'keemari', 'korangi', 'malir', 'quaideen', 
            'sharqi', 'shumali', 'site_gharbi',
            'ismail', 'ahsan', 'farhzan', 'faizan', 'bashar'
        ],
        'password': [
            'admin123', 'admin2123',
            'airport123', 'gadap123', 'gharbi123', 'wasti123', 'gulberg123',
            'junoobi123', 'keemari123', 'korangi123', 'malir123', 'quaideen123',
            'sharqi123', 'shumali123', 'site123',
            'ismail123', 'ahsan123', 'farhzan123', 'faizan123', 'bashar123'
        ],
        'zila': [
            'کراچی مرکز', 'کراچی مرکز',
            'ایئرپورٹ', 'گڈاپ', 'غربی', 'وسطی', 'گلبرگ وسطی',
            'جنوبی', 'کیماڑی', 'کورنگی', 'ملیر', 'قائدین',
            'شرقی', 'شمالی', 'سائٹ غربی',
            'کراچی مرکز', 'کراچی مرکز', 'کراچی مرکز', 'کراچی مرکز', 'کراچی مرکز'
        ],
        'role': [
            'admin', 'admin2',
            'user', 'user', 'user', 'user', 'user',
            'user', 'user', 'user', 'user', 'user',
            'user', 'user', 'user',
            'agent', 'agent', 'agent', 'agent', 'agent'
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
        
        # Get the period from new_entry or default to Q2 2025
        period = new_entry.get('period', 'Q2 2025')
        
        # Get actual columns in the table
        c.execute('PRAGMA table_info(monthly_reports)')
        table_columns = [row[1] for row in c.fetchall()]
        
        # Only keep keys that are real columns and filter out dynamic fields
        filtered_entry = {}
        for k, v in new_entry.items():
            if k in table_columns and not k.startswith('atifal_programat_') and not k.startswith('programat_'):
                filtered_entry[k] = v
        
        # Check if a record already exists for this zila and period
        c.execute('SELECT COUNT(*) FROM monthly_reports WHERE zila=? AND period=?', 
                 (filtered_entry['zila'], period))
        exists = c.fetchone()[0] > 0
        
        if exists:
            # Update existing record for this specific period
            set_clause = ', '.join([f'{k}=?' for k in filtered_entry.keys()])
            values = list(filtered_entry.values())
            values.extend([filtered_entry['zila'], period])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
            c.execute(sql, values)
            logging.info(f"Updated existing report for zila={filtered_entry['zila']}, period={period}")
        else:
            # Insert new record for this specific period
            keys = list(filtered_entry.keys())
            values = [filtered_entry[k] for k in keys]
            placeholders = ','.join(['?'] * len(keys))
            sql = f"INSERT INTO monthly_reports ({','.join(keys)}) VALUES ({placeholders})"
            c.execute(sql, values)
            logging.info(f"Inserted new report for zila={filtered_entry['zila']}, period={period}")
        
        conn.commit()
        c.execute('SELECT * FROM monthly_reports WHERE zila=? AND period=?', 
                 (filtered_entry['zila'], period))
        conn.close()
        logging.info("Report saved successfully.")
    except Exception as e:
        logging.error(f"Error in save_report_to_db: {e}", exc_info=True)
        raise

def update_report_in_db(zila, new_entry):
    import logging
    try:
        logging.info(f"Attempting to update report for zila={zila}: {new_entry}")
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get the period from new_entry or default to Q2 2025
        period = new_entry.get('period', 'Q2 2025')
        
        set_clause = ', '.join([f'{k}=?' for k in new_entry.keys()])
        values = list(new_entry.values())
        values.extend([zila, period])  # Add zila and period to values
        sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
        c.execute(sql, values)
        conn.commit()
        c.execute('SELECT * FROM monthly_reports WHERE zila=? AND period=?', (zila, period))
        conn.close()
        logging.info(f"Report updated successfully for zila={zila}, period={period}.")
    except Exception as e:
        logging.error(f"Error in update_report_in_db: {e}", exc_info=True)
        raise

# Load initial data
users_df = load_users_from_db()
report_df = load_reports_from_db()

def get_q2_2025_data(zila):
    """Helper function to get Q2 2025 data for a zila"""
    try:
        global report_df
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q2 2025')]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[(report_df['zila'].str.strip() == normalized_zila) & (report_df['period'] == 'Q2 2025')]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[(report_df['zila'] == db_zila) & (report_df['period'] == 'Q2 2025')]
                        break
        
        return report_row
    except Exception as e:
        import logging
        logging.error(f"Error getting Q2 2025 data: {e}", exc_info=True)
        return pd.DataFrame()

def get_next_quarter_ikhtitam(zila, target_quarter, target_year):
    """Get ikhtitam values from the next quarter to use as aghaz for the target quarter"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Calculate the next quarter
        quarter_map = {'Q1': 'Q2', 'Q2': 'Q3', 'Q3': 'Q4', 'Q4': 'Q1'}
        next_quarter = quarter_map.get(target_quarter, 'Q1')
        
        if target_quarter == 'Q4':
            next_year = target_year + 1
        else:
            next_year = target_year
        
        next_period = f"{next_quarter} {next_year}"
        
        # Get the next quarter's data
        c.execute('''
            SELECT * FROM monthly_reports 
            WHERE zila = ? AND period = ?
        ''', (zila, next_period))
        
        next_record = c.fetchone()
        
        # Also check what periods exist for this zila
        c.execute('''
            SELECT period FROM monthly_reports 
            WHERE zila = ? AND period IS NOT NULL AND period != 'None None'
        ''', (zila,))
        all_periods = c.fetchall()
        
        conn.close()
        
        if not next_record:
            # No next quarter data, return empty values
            return {}
        
        # Get column names
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("PRAGMA table_info(monthly_reports)")
        columns = [row[1] for row in c.fetchall()]
        conn.close()
        
        # Create a dictionary of the next record data
        next_data = dict(zip(columns, next_record))
        
        # Extract ikhtitam values to use as aghaz
        ikhtitam_fields = {}
        for key, value in next_data.items():
            if key.endswith('_ikhtitaam') and value is not None:
                # Convert ikhtitam field name to aghaz field name
                aghaz_field = key.replace('_ikhtitaam', '_start')
                ikhtitam_fields[aghaz_field] = value
            elif key.endswith('_ikhtitam') and value is not None:  # Try alternative spelling
                # Convert ikhtitam field name to aghaz field name
                aghaz_field = key.replace('_ikhtitam', '_start')
                ikhtitam_fields[aghaz_field] = value
        
        return ikhtitam_fields
        
    except Exception as e:
        import logging
        logging.error(f"Error getting next quarter ikhtitam for zila: {e}", exc_info=True)
        return {}

def get_previous_quarter_for_zila(zila):
    """Determine the previous quarter to fill in for a zila based on existing data"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get all periods for this zila
        c.execute('''
            SELECT period FROM monthly_reports 
            WHERE zila = ? AND period IS NOT NULL AND period != 'None None'
        ''', (zila,))
        
        all_periods = c.fetchall()
        conn.close()
        
        # Parse and sort periods chronologically (highest year, then highest quarter)
        valid_periods = []
        for row in all_periods:
            period = row[0]
            if period and ' ' in period:
                try:
                    quarter, year_str = period.split(' ')
                    year = int(year_str)
                    valid_periods.append((period, quarter, year))
                except ValueError:
                    continue
        
        # Sort by year (descending) then by quarter (Q4, Q3, Q2, Q1)
        quarter_order = {'Q4': 4, 'Q3': 3, 'Q2': 2, 'Q1': 1}
        valid_periods.sort(key=lambda x: (x[2], quarter_order.get(x[1], 0)), reverse=True)
        
        existing_periods = [(period,) for period, _, _ in valid_periods]
        
        if not existing_periods:
            # If no data exists, start with Q1 2025
            return 'Q1', '2025'
        
        # Find the next missing quarter to fill in
        # Start from the earliest existing quarter and work backwards
        existing_quarters = set()
        for period, quarter, year in valid_periods:
            existing_quarters.add((quarter, year))
        
        # Find the earliest existing quarter
        earliest_period = valid_periods[-1]  # Last in sorted list (earliest)
        earliest_quarter, earliest_year = earliest_period[1], earliest_period[2]
        
        # Calculate the previous quarter to the earliest one
        quarter_map = {'Q2': 'Q1', 'Q3': 'Q2', 'Q4': 'Q3', 'Q1': 'Q4'}
        previous_quarter = quarter_map.get(earliest_quarter, 'Q4')
        
        if earliest_quarter == 'Q1':
            previous_year = earliest_year - 1
        else:
            previous_year = earliest_year
        
        # Check if this suggested quarter already exists
        suggested_quarter = (previous_quarter, previous_year)
        
        if suggested_quarter in existing_quarters:
            # If the suggested quarter already exists, find the next missing one
            # Start from Q1 2023 and work forward until we find a missing quarter
            test_quarter = 'Q1'
            test_year = 2023
            
            while test_year <= earliest_year:
                while test_quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
                    if (test_quarter, test_year) not in existing_quarters:
                        return test_quarter, str(test_year)
                    
                    # Move to next quarter
                    if test_quarter == 'Q1':
                        test_quarter = 'Q2'
                    elif test_quarter == 'Q2':
                        test_quarter = 'Q3'
                    elif test_quarter == 'Q3':
                        test_quarter = 'Q4'
                    elif test_quarter == 'Q4':
                        test_quarter = 'Q1'
                        test_year += 1
                        break
                
                if test_year > earliest_year:
                    break
        else:
            pass
        
        return previous_quarter, str(previous_year)
            
    except Exception as e:
        import logging
        logging.error(f"Error determining previous quarter for zila: {e}", exc_info=True)
        return 'Q1', '2025'

def get_form_data(zila):
    """Get data for form fields from SQLite DB"""
    try:
        # Reload data from DB
        global report_df
        report_df = load_reports_from_db()
        # Get data for this zila (only Q2 2025 reports for users)
        # Users submit Q2 2025 reports, agents submit historical data
        zila_data = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q2 2025')]
        
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
            
            # Load youth programs from JSON and populate individual fields
            youth_programs_json = form_data.get('youth_programs_json', '')
            if youth_programs_json:
                try:
                    youth_programs = json.loads(youth_programs_json)
                    # Set the count to the actual number of programs in JSON
                    form_data['youth_programat_count'] = str(len(youth_programs))
                    for idx, prog in enumerate(youth_programs):
                        form_data[f'programat_{idx+1}'] = prog.get('name', '')
                        form_data[f'programat_count_{idx+1}'] = prog.get('count', '')
                except Exception as e:
                    # If JSON parsing fails, fall back to individual fields
                    for i in range(1, 51):
                        program_name = first_row.get(f'programat_{i}', '')
                        program_count = first_row.get(f'programat_count_{i}', '')
                        form_data[f'programat_{i}'] = '' if pd.isna(program_name) else str(program_name)
                        form_data[f'programat_count_{i}'] = '' if pd.isna(program_count) else str(program_count)
            else:
                # No JSON data, load from individual fields
                for i in range(1, 51):
                    program_name = first_row.get(f'programat_{i}', '')
                    program_count = first_row.get(f'programat_count_{i}', '')
                    form_data[f'programat_{i}'] = '' if pd.isna(program_name) else str(program_name)
                    form_data[f'programat_count_{i}'] = '' if pd.isna(program_count) else str(program_count)
            
            # Load atifal programs from JSON and populate individual fields
            atifal_programs_json = form_data.get('atifal_programs_json', '')
            if atifal_programs_json:
                try:
                    atifal_programs = json.loads(atifal_programs_json)
                    # Set the count to the actual number of programs in JSON
                    form_data['atifal_programat_count'] = str(len(atifal_programs))
                    for idx, prog in enumerate(atifal_programs):
                        form_data[f'atifal_nauyiat_{idx+1}'] = prog.get('nauiyat', '')
                        form_data[f'atifal_programat_count_{idx+1}'] = prog.get('hazri', '')
                except Exception as e:
                    # If JSON parsing fails, fall back to individual fields
                    for i in range(1, 51):
                        atifal_nauiyat = first_row.get(f'atifal_nauyiat_{i}', '')
                        atifal_hazri = first_row.get(f'atifal_programat_count_{i}', '')
                        form_data[f'atifal_nauyiat_{i}'] = '' if pd.isna(atifal_nauiyat) else str(atifal_nauiyat)
                        form_data[f'atifal_programat_count_{i}'] = '' if pd.isna(atifal_hazri) else str(atifal_hazri)
            else:
                # No JSON data, load from individual fields
                for i in range(1, 51):
                    atifal_nauiyat = first_row.get(f'atifal_nauyiat_{i}', '')
                    atifal_hazri = first_row.get(f'atifal_programat_count_{i}', '')
                    form_data[f'atifal_nauyiat_{i}'] = '' if pd.isna(atifal_nauiyat) else str(atifal_nauiyat)
                    form_data[f'atifal_programat_count_{i}'] = '' if pd.isna(atifal_hazri) else str(atifal_hazri)
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
            # Redirect admin, admin2, and agent users to dashboard, others to report
            if user.iloc[0]['role'] in ['admin', 'admin2', 'agent']:
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
        # Redirect admin2 and agent users to dashboard since they should only view reports, not edit them
        if session.get('role') in ['admin2', 'agent']:
            return redirect(url_for('dashboard'))
        zila = session['zila']
        
        # Set the current reporting period to Q2 2025
        current_period = "Q2 2025"
        global report_df
        report_df = load_reports_from_db()
        existing_report = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q2 2025')]
        report_exists = not existing_report.empty
        form_data = get_form_data(zila)
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
        
        # Add dynamic program fields to visible_fields for locking
        for i in range(1, 51):
            visible_fields.extend([
                f'programat_{i}', f'programat_count_{i}',
                f'atifal_nauyiat_{i}', f'atifal_programat_count_{i}'
            ])
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
            new_entry['timestamp'] = datetime.now().isoformat()
            new_entry['submitted_by'] = session['username']
            new_entry['last_submitted_by'] = session.get('role', 'user')
            new_entry['period'] = current_period  # Automatically set to Q2 2025
            update_fields = {}
            if session.get('role') == 'admin':
                admin_editable_fields = [
                    'alaqajat_target', 'alaqajat_start', 'alaqajat_ikhtitaam', 'halqajat_target', 'halqajat_start', 'halqajat_ikhtitaam', 'halqajat_ward_target', 'halqajat_ward_start', 'halqajat_ward_ikhtitaam',
                    'block_code_target', 'block_code_start', 'block_code_ikhtitaam', 'nizam_e_fajar_target', 'nizam_e_fajar_start', 'nizam_e_fajar_ikhtitaam', 'awaami_committee_target', 'awaami_committee_start', 'awaami_committee_ikhtitaam',
                    'arkaan_target', 'arkaan_start', 'arkaan_ikhtitaam', 'umeedwaran_target', 'umeedwaran_start', 'umeedwaran_ikhtitaam', 'karkunan_target', 'karkunan_start', 'karkunan_ikhtitaam', 'hangami_target', 'hangami_start', 'hangami_ikhtitaam',
                    'muawanin_target', 'muawanin_start', 'muawanin_ikhtitaam', 'mutayyin_afrad_target', 'mutayyin_afrad_start', 'mutayyin_afrad_ikhtitaam', 'member_target', 'member_start', 'member_ikhtitaam',
                    'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count'
                ]
                for key in admin_editable_fields:
                    if key in new_entry:
                        update_fields[key] = new_entry[key]
                update_fields['zila'] = zila
                update_fields['timestamp'] = new_entry['timestamp']
                update_fields['submitted_by'] = new_entry['submitted_by']
                update_fields['last_submitted_by'] = new_entry['last_submitted_by']
                update_fields['period'] = current_period  # Automatically set to Q2 2025
                if report_exists:
                    update_report_in_db(zila, update_fields)
                    flash("Updated successfully!", "success")
                else:
                    save_report_to_db(update_fields)
                    flash("Report submitted!", "success")
                return redirect(url_for('report'))
            # ... rest of user logic ...
            for key in visible_fields:
                if session.get('role') != 'admin':
                    form_value = new_entry.get(key, '').strip() if isinstance(new_entry.get(key, ''), str) else new_entry.get(key, '')
                    if report_exists:
                        row = existing_report.iloc[0]
                        db_value = str(row.get(key, '')).strip() if row.get(key, '') is not None else ''
                    else:
                        db_value = ''
                    if db_value in ['', 'nan', 'None'] and form_value not in ['', 'nan', 'None']:
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
                
                # Always update youth_programs_json for users if present
                if session.get('role') != 'admin' and 'youth_programs_json' in new_entry:
                    update_fields['youth_programs_json'] = new_entry['youth_programs_json']
                # Always update atifal_programat_count for users if present (like youth_programat_count)
                if session.get('role') != 'admin' and 'atifal_programat_count' in new_entry:
                    try:
                        update_fields['atifal_programat_count'] = str(int(new_entry['atifal_programat_count']))
                    except Exception:
                        update_fields['atifal_programat_count'] = '0'
                
                # Always update atifal_programs_json for users if present
                if session.get('role') != 'admin' and 'atifal_programs_json' in new_entry:
                    update_fields['atifal_programs_json'] = new_entry['atifal_programs_json']
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
                update_fields['period'] = current_period  # Automatically set to Q2 2025
                if update_fields:
                    update_report_in_db(zila, update_fields)
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
                # Check if report already exists for this zila (Q2 2025)
                existing_report = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q2 2025')]
                if not existing_report.empty:
                    # Update existing report
                    update_report_in_db(zila, new_entry)
                    flash("Report updated successfully!", "success")
                else:
                    # Create new report
                    save_report_to_db(new_entry)
                    flash("Report submitted!", "success")
            return redirect(url_for('report'))
        # For GET: Check if report is locked by admin
        locked_fields = {}
        is_report_locked = False
        if report_exists:
            row = existing_report.iloc[0]
            # Check if report is locked by admin
            is_report_locked = bool(row.get('is_locked', 0))
            
            # If report is locked by admin, lock all fields for non-admin users
            if is_report_locked and session.get('role') != 'admin':
                for key in visible_fields:
                    locked_fields[key] = True
            else:
                # No locking - allow editing of all fields
                for key in visible_fields:
                    locked_fields[key] = False
        
        # Get form data (now includes properly loaded JSON data)
        form_data = get_form_data(zila)
        
        # Create program lists for template rendering
        programat_list = []
        programat_json = form_data.get('youth_programs_json', '')
        if programat_json:
            try:
                programat_list = json.loads(programat_json)
            except Exception:
                programat_list = []
        
        atifal_programs_list = []
        atifal_programs_json = form_data.get('atifal_programs_json', '')
        if atifal_programs_json:
            try:
                atifal_programs_list = json.loads(atifal_programs_json)
            except Exception:
                atifal_programs_list = []
        
        # Set locking for program fields based on form_data (which now has the correct values)
        current_user = session.get('username', '')
        submitted_by = form_data.get('submitted_by', '')
        
        # Lock youth program fields if they have data and current user submitted them
        for i in range(1, 51):
            program_name = form_data.get(f'programat_{i}', '')
            program_count = form_data.get(f'programat_count_{i}', '')
            if program_name and str(program_name).strip() not in ['', 'nan', 'None']:
                locked_fields[f'programat_{i}'] = (current_user == submitted_by)
            if program_count and str(program_count).strip() not in ['', '0', 'nan', 'None']:
                locked_fields[f'programat_count_{i}'] = (current_user == submitted_by)
        
        # Lock atifal program fields if they have data and current user submitted them
        for i in range(1, 51):
            atifal_nauiyat = form_data.get(f'atifal_nauyiat_{i}', '')
            atifal_hazri = form_data.get(f'atifal_programat_count_{i}', '')
            if atifal_nauiyat and str(atifal_nauiyat).strip() not in ['', 'nan', 'None']:
                locked_fields[f'atifal_nauyiat_{i}'] = (current_user == submitted_by)
            if atifal_hazri and str(atifal_hazri).strip() not in ['', '0', 'nan', 'None']:
                locked_fields[f'atifal_programat_count_{i}'] = (current_user == submitted_by)
        
        # Lock program count fields if they have data and current user submitted them
        if form_data.get('youth_programat_count') and str(form_data.get('youth_programat_count')).strip() not in ['', '0', 'nan', 'None']:
            locked_fields['youth_programat_count'] = (current_user == submitted_by)
        
        if form_data.get('atifal_programat_count') and str(form_data.get('atifal_programat_count')).strip() not in ['', '0', 'nan', 'None']:
            locked_fields['atifal_programat_count'] = (current_user == submitted_by)
        

        
        youth_count = form_data.get('youth_programat_count', '')
        youth_count_has_data = youth_count and str(youth_count).strip() not in ['', '0', 'nan', 'None']
        locked_fields['youth_programat_count'] = youth_count_has_data and (current_user == submitted_by)
        
        atifal_count = form_data.get('atifal_programat_count', '')
        atifal_count_has_data = atifal_count and str(atifal_count).strip() not in ['', '0', 'nan', 'None']
        locked_fields['atifal_programat_count'] = atifal_count_has_data and (current_user == submitted_by)
        
        # Set can_submit based on whether report is locked
        can_submit = not is_report_locked or session.get('role') == 'admin'
        
        # Debug: Print locked fields for atifal programs
        atifal_locked_fields = {k: v for k, v in locked_fields.items() if 'atifal' in k}
        
        return render_template("report.html", 
                             zila=zila, 
                             form_data=form_data,
                             report_exists=report_exists,
                             programat_list=programat_list,
                             locked_fields=locked_fields,
                             can_submit=can_submit,
                             is_view_mode=False,
                             is_admin_target_setting=False,
                             is_report_locked=is_report_locked)
    except Exception as e:
        import logging
        logging.error("Exception in /report route", exc_info=True)
        return "Internal Server Error. Please check error.log for details.", 500

@app.route('/dashboard')
def dashboard():
    try:
        if 'username' not in session:
            return redirect(url_for('login'))
        if session.get('role') not in ['admin', 'admin2', 'agent']:
            flash("آپ کو ڈیش بورڈ دیکھنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('report'))
        global report_df, users_df
        report_df = load_reports_from_db()
        users_df = load_users_from_db()
        # Filter to show only Q2 2025 reports for dashboard
        current_reports = report_df[report_df['period'] == 'Q2 2025']  # Only Q2 2025 reports
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
            report_row = pd.DataFrame(current_reports[(current_reports['zila'] == zila) & (current_reports['period'] == 'Q2 2025')])
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
                
                # Add additional data for dashboard display
                zila_dict['arkaan_end'] = row.get('arkaan_end', '')
                zila_dict['arkaan_target'] = row.get('arkaan_target', '')
                zila_dict['member_end'] = row.get('member_end', '')
                zila_dict['member_target'] = row.get('member_target', '')
                zila_dict['timestamp'] = row.get('timestamp', '')
                
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
                # Add lock status
                zila_dict['is_locked'] = bool(row.get('is_locked', 0))
            else:
                zila_dict['status'] = 'not_started'
                zila_dict['has_report'] = False
                zila_dict['is_locked'] = False
            zila_status.append(zila_dict)
        
        # Calculate summary statistics
        total_submitted = sum(1 for zila in zila_status if zila['status'] == 'submitted')
        total_partial = sum(1 for zila in zila_status if zila['status'] == 'partial')
        total_not_started = sum(1 for zila in zila_status if zila['status'] == 'not_started')
        
        # Calculate totals for key metrics
        total_arkaan = 0
        total_members = 0
        total_arkaan_target = 0
        total_members_target = 0
        
        for zila in zila_status:
            if zila['has_report']:
                # Get the actual report data (Q2 2025)
                report_row = current_reports[(current_reports['zila'] == zila['zila']) & (current_reports['period'] == 'Q2 2025')]
                if not report_row.empty:
                    row = report_row.iloc[0]
                    # Sum up arkaan and members
                    arkaan_end = row.get('arkaan_end', 0) or 0
                    member_end = row.get('member_end', 0) or 0
                    arkaan_target = row.get('arkaan_target', 0) or 0
                    member_target = row.get('member_target', 0) or 0
                    
                    # Handle NaN values properly
                    arkaan_end_val = 0 if pd.isna(arkaan_end) else (int(arkaan_end) if arkaan_end else 0)
                    member_end_val = 0 if pd.isna(member_end) else (int(member_end) if member_end else 0)
                    arkaan_target_val = 0 if pd.isna(arkaan_target) else (int(arkaan_target) if arkaan_target else 0)
                    member_target_val = 0 if pd.isna(member_target) else (int(member_target) if member_target else 0)
                    
                    total_arkaan += arkaan_end_val
                    total_members += member_end_val
                    total_arkaan_target += arkaan_target_val
                    total_members_target += member_target_val
        
        summary = {
            'total_zilas': len(all_zilas),
            'total_submitted_reports': total_submitted,
            'total_partial_reports': total_partial,
            'total_not_submitted_reports': total_not_started,
            'total_arkaan': total_arkaan,
            'total_members': total_members,
            'total_arkaan_target': total_arkaan_target,
            'total_members_target': total_members_target,
            'completion_percentage': round((total_submitted / len(all_zilas)) * 100, 1) if all_zilas else 0
        }
        
        return render_template("dashboard.html", 
                             summary=summary, 
                             zila_status=zila_status,
                             current_month=datetime.today().strftime('%Y-%m'))
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
        zila = request.form.get('zila')
        ibtidai_maloomat_tadad = request.form.get('ibtidai_maloomat_tadad') or None
        tanzeemi_hayat = request.form.get('tanzeemi_hayat') or None
        afradi_quwat = request.form.get('afradi_quwat') or None
        update_fields = {
            'ibtidai_maloomat_tadad': ibtidai_maloomat_tadad,
            'tanzeemi_hayat': tanzeemi_hayat,
            'afradi_quwat': afradi_quwat
        }
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        # Check for existing record with Q2 2025 period
        c.execute('SELECT id FROM monthly_reports WHERE zila=? AND period=?', (zila, 'Q2 2025'))
        row = c.fetchone()
        if row:
            # Update existing record for Q2 2025
            set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
            values = list(update_fields.values())
            values.extend([zila, 'Q2 2025'])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
            c.execute(sql, values)
        else:
            # Insert new record for Q2 2025
            sql = "INSERT INTO monthly_reports (zila, ibtidai_maloomat_tadad, tanzeemi_hayat, afradi_quwat, timestamp, submitted_by, period) VALUES (?, ?, ?, ?, datetime('now'), ?, ?)"
            c.execute(sql, (zila, ibtidai_maloomat_tadad, tanzeemi_hayat, afradi_quwat, session.get('username', 'admin'), 'Q2 2025'))
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
            # Check for existing record with Q2 2025 period
            c.execute('SELECT id FROM monthly_reports WHERE zila=? AND period=?', (zila, 'Q2 2025'))
            row = c.fetchone()
            if row:
                # Update existing record for Q2 2025
                set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
                values = list(update_fields.values())
                values.extend([zila, 'Q2 2025'])
                sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
                c.execute(sql, values)
            else:
                # Insert new record for Q2 2025
                sql = "INSERT INTO monthly_reports (zila, union_committee_count, wards_count, block_code_count, cantonment_board_count, timestamp, submitted_by, period) VALUES (?, ?, ?, ?, ?, datetime('now'), ?, ?)"
                c.execute(sql, (zila, union_committee_count, wards_count, block_code_count, cantonment_board_count, session.get('username', 'admin'), 'Q2 2025'))
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
        # Check for existing record with Q2 2025 period
        c.execute('SELECT id FROM monthly_reports WHERE zila=? AND period=?', (zila, 'Q2 2025'))
        row = c.fetchone()
        if row:
            # Update existing record for Q2 2025
            set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
            values = list(update_fields.values())
            values.extend([zila, 'Q2 2025'])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
            c.execute(sql, values)
        else:
            # Insert new record for Q2 2025
            sql = f"INSERT INTO monthly_reports (zila, {', '.join(update_fields.keys())}, timestamp, submitted_by, period) VALUES (?, {', '.join(['?']*len(update_fields))}, datetime('now'), ?, ?)"
            c.execute(sql, (zila, *update_fields.values(), session.get('username', 'admin'), 'Q2 2025'))
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
        # Check for existing record with Q2 2025 period
        c.execute('SELECT id FROM monthly_reports WHERE zila=? AND period=?', (zila, 'Q2 2025'))
        row = c.fetchone()
        if row:
            # Update existing record for Q2 2025
            set_clause = ', '.join([f'{k}=?' for k in update_fields.keys()])
            values = list(update_fields.values())
            values.extend([zila, 'Q2 2025'])
            sql = f"UPDATE monthly_reports SET {set_clause} WHERE zila=? AND period=?"
            c.execute(sql, values)
        else:
            # Insert new record for Q2 2025
            sql = f"INSERT INTO monthly_reports (zila, {', '.join(update_fields.keys())}, timestamp, submitted_by, period) VALUES (?, {', '.join(['?']*len(update_fields))}, datetime('now'), ?, ?)"
            c.execute(sql, (zila, *update_fields.values(), session.get('username', 'admin'), 'Q2 2025'))
        conn.commit()
        conn.close()
        flash('افرادی قوت کامیابی سے محفوظ ہو گئیں۔')
        return redirect(url_for('dashboard'))
    except Exception as e:
        import logging
        logging.error("Exception in /update_manpower route", exc_info=True)
        flash('محفوظ کرنے میں خرابی۔')
        return redirect(url_for('dashboard'))

@app.route('/view_report/<zila>')
def view_report(zila):
    try:
        if 'username' not in session:
            return redirect(url_for('login'))
        
        # Allow access if user is admin/admin2 OR if user is viewing their own zila
        user_role = session.get('role')
        user_zila = session.get('zila')
        
        # Decode URL if needed
        from urllib.parse import unquote
        zila = unquote(zila)
        
        # Check access: admin/admin2 can view any zila, regular users can only view their own
        if user_role not in ['admin', 'admin2'] and user_zila != zila:
            flash("آپ کو صرف اپنی ضلع کی رپورٹ دیکھنے کی اجازت ہے", "error")
            return redirect(url_for('report'))
        
        global report_df
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[report_df['zila'] == zila]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[report_df['zila'].str.strip() == normalized_zila]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[report_df['zila'] == db_zila]
                        logging.info(f"Found partial match: {db_zila} for {normalized_zila}")
                        break
        
        if report_row.empty:
            # No report exists, create empty form data
            form_data = get_form_data(zila)  # This will initialize all required fields
            report_exists = False
        else:
            # Report exists, get the data
            form_data = report_row.iloc[0].to_dict()
            # Convert NaN to empty string
            form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
            report_exists = True
        
        # Calculate report completion status
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
        
        if report_exists:
            total_fields = len(visible_fields)
            filled_fields = sum(1 for field in visible_fields if str(form_data.get(field, '')).strip() not in ['', 'nan', 'None'])
            
            if filled_fields == 0:
                report_status = 'not_started'
                status_text = 'شروع نہیں ہوئی'
            elif filled_fields == total_fields:
                report_status = 'submitted'
                status_text = 'مکمل جمع'
            else:
                report_status = 'partial'
                status_text = 'جزوی جمع'
            
            completion_percentage = round((filled_fields / total_fields) * 100, 1)
        else:
            # No report exists
            report_status = 'not_started'
            status_text = 'رپورٹ موجود نہیں'
            completion_percentage = 0
            filled_fields = 0
            total_fields = len(visible_fields)
        
        # All fields locked (read-only for admin view)
        # Initialize locked_fields with all possible form fields
        all_possible_fields = [
            'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
            'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
            'alaqajat_start', 'alaqajat_end', 'alaqajat_target', 'alaqajat_izafa', 'alaqajat_kami', 'alaqajat_ikhtitaam',
            'halqajat_start', 'halqajat_end', 'halqajat_target', 'halqajat_izafa', 'halqajat_kami', 'halqajat_ikhtitaam',
            'halqajat_ward_start', 'halqajat_ward_end', 'halqajat_ward_target', 'halqajat_ward_izafa', 'halqajat_ward_kami', 'halqajat_ward_ikhtitaam',
            'block_code_start', 'block_code_end', 'block_code_target', 'block_code_izafa', 'block_code_kami', 'block_code_ikhtitaam',
            'arkaan_start', 'arkaan_end', 'arkaan_target', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
            'umeedwaran_start', 'umeedwaran_end', 'umeedwaran_target', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
            'karkunan_start', 'karkunan_end', 'karkunan_target', 'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam',
            'hangami_start', 'hangami_end', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
            'muawanin_start', 'muawanin_end', 'muawanin_target', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
            'mutayyin_afrad_start', 'mutayyin_afrad_end', 'mutayyin_afrad_target', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
            'member_start', 'member_end', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam',
            'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'youth_programs_json',
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
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
            'nizam_e_fajar_start', 'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
            'awaami_committee_start', 'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
        ]
        
        # Lock all fields for admin view (read-only)
        locked_fields = {field: True for field in all_possible_fields}
        
        # Load youth programs
        programat_list = []
        if report_exists:
            programat_json = form_data.get('youth_programs_json', '')
            if programat_json:
                try:
                    programat_list = json.loads(programat_json)
                except Exception:
                    programat_list = []
        
        # Load atifal programs list from JSON
        atifal_programs_list = []
        if report_exists:
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
            # Lock dynamic atifal fields
            locked_fields[f'atifal_nauyiat_{idx+1}'] = True
            locked_fields[f'atifal_programat_count_{idx+1}'] = True
        
        # Lock dynamic youth program fields
        for idx, prog in enumerate(programat_list):
            locked_fields[f'programat_{idx+1}'] = True
            locked_fields[f'programat_count_{idx+1}'] = True
        
        # Add report metadata
        report_metadata = {
            'status': report_status,
            'status_text': status_text,
            'completion_percentage': completion_percentage,
            'filled_fields': filled_fields,
            'total_fields': total_fields,
            'submitted_by': form_data.get('submitted_by', ''),
            'timestamp': form_data.get('timestamp', ''),
            'last_submitted_by': form_data.get('last_submitted_by', '')
        }
        
        return render_template("report.html", 
                             zila=zila, 
                             form_data=form_data,
                             report_exists=report_exists,
                             programat_list=programat_list,
                             atifal_programs_list=atifal_programs_list,
                             locked_fields=locked_fields,
                             can_submit=False,
                             report_metadata=report_metadata,
                             is_view_mode=True,
                             is_admin_target_setting=False,
                             hide_header=request.args.get('hide_header', False))
    except Exception as e:
        import logging
        logging.error("Exception in /view_report route", exc_info=True)
        flash("رپورٹ دیکھنے میں خرابی۔", "error")
        return redirect(url_for('dashboard'))


@app.route('/print_report/<zila>')
def print_report(zila):
    global report_df  # Move to top to avoid SyntaxError
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash("آپ کو پرنٹ رپورٹ کی اجازت نہیں ہے", "error")
            return redirect(url_for('dashboard'))
        
        # Decode URL if needed
        from urllib.parse import unquote
        zila = unquote(zila)
        
        # Load report data
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[report_df['zila'] == zila]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[report_df['zila'].str.strip() == normalized_zila]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[report_df['zila'] == db_zila]
                        logging.info(f"Found partial match: {db_zila} for {normalized_zila}")
                        break
        
        if report_row.empty:
            form_data = get_form_data(zila)
            report_exists = False
        else:
            form_data = report_row.iloc[0].to_dict()
            form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
            report_exists = True
        
        # Calculate report completion status
        visible_fields = [
            'union_committee_count', 'nazm_qaim_union', 'wards_count', 'nazm_qaim_wards',
            'block_code_count', 'nazm_qaim_blockcode', 'cantonment_board_count', 'nazm_qaim_cantonment',
            'arkaan_target', 'arkaan_start', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
            'umeedwaran_target', 'umeedwaran_start', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
            'karkunan_target', 'karkunan_start', 'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam',
            'hangami_target', 'hangami_start', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
            'muawanin_target', 'muawanin_start', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
            'mutayyin_afrad_target', 'mutayyin_afrad_start', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
            'member_target', 'member_start', 'member_izafa', 'member_kami', 'member_ikhtitaam'
        ]
        
        filled_fields = sum(1 for field in visible_fields if form_data.get(field) and str(form_data.get(field)).strip())
        completion_percentage = round((filled_fields / len(visible_fields)) * 100, 1) if visible_fields else 0
        
        if completion_percentage == 0:
            status_text = "شروع نہیں ہوا"
        elif completion_percentage < 100:
            status_text = "جزوی مکمل"
        else:
            status_text = "مکمل"
        
        report_metadata = {
            'status_text': status_text,
            'completion_percentage': completion_percentage,
            'timestamp': form_data.get('timestamp', 'نامعلوم تاریخ'),
            'submitted_by': form_data.get('submitted_by', '—')
        }
        
        # Return print-friendly HTML page with Urdu text
        return render_template('print_report.html', zila=zila, form_data=form_data, report_metadata=report_metadata)
    except Exception as e:
        import logging
        logging.error("Exception in /print_report route", exc_info=True)
        flash("Error generating print report", "error")
        return redirect(url_for('dashboard'))







@app.route('/set_zila_and_redirect', methods=['POST'])
def set_zila_and_redirect():
    if 'username' not in session or session.get('role') != 'admin':
        flash("آپ کو یہ عمل کرنے کی اجازت نہیں ہے", "error")
        return redirect(url_for('dashboard'))
    zila = request.form.get('zila')
    if zila:
        session['zila'] = zila
    return redirect(url_for('report'))

@app.route('/admin_target_setting/<zila>', methods=['GET', 'POST'])
def admin_target_setting(zila):
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash("آپ کو ہدف درج کرنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('dashboard'))
        
        # Decode URL if needed
        from urllib.parse import unquote
        zila = unquote(zila)
        
        global report_df
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[report_df['zila'] == zila]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[report_df['zila'].str.strip() == normalized_zila]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[report_df['zila'] == db_zila]
                        logging.info(f"Found partial match: {db_zila} for {normalized_zila}")
                        break
        
        if report_row.empty:
            # No report exists, create empty form data
            form_data = get_form_data(zila)
            report_exists = False
        else:
            # Report exists, get the data
            form_data = report_row.iloc[0].to_dict()
            form_data = {k: ('' if pd.isna(v) else v) for k, v in form_data.items()}
            report_exists = True
        
        # For admin target setting, only lock non-target fields
        # Admin can edit target fields and start fields
        locked_fields = {}
        
        # Define which fields admin can edit (targets, starts, and ikhtitaam)
        admin_editable_fields = [
            'alaqajat_target', 'alaqajat_start', 'alaqajat_ikhtitaam', 'halqajat_target', 'halqajat_start', 'halqajat_ikhtitaam', 
            'halqajat_ward_target', 'halqajat_ward_start', 'halqajat_ward_ikhtitaam', 'block_code_target', 'block_code_start', 'block_code_ikhtitaam', 
            'nizam_e_fajar_target', 'nizam_e_fajar_start', 'nizam_e_fajar_ikhtitaam', 'awaami_committee_target', 'awaami_committee_start', 'awaami_committee_ikhtitaam',
            'arkaan_target', 'arkaan_start', 'arkaan_ikhtitaam', 'umeedwaran_target', 'umeedwaran_start', 'umeedwaran_ikhtitaam', 
            'karkunan_target', 'karkunan_start', 'karkunan_ikhtitaam', 'hangami_target', 'hangami_start', 'hangami_ikhtitaam',
            'muawanin_target', 'muawanin_start', 'muawanin_ikhtitaam', 'mutayyin_afrad_target', 'mutayyin_afrad_start', 'mutayyin_afrad_ikhtitaam', 
            'member_target', 'member_start', 'member_ikhtitaam', 'union_committee_count', 'wards_count', 
            'block_code_count', 'cantonment_board_count'
        ]
        
        # Lock all fields except admin editable ones
        all_possible_fields = [
            'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
            'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
            'alaqajat_start', 'alaqajat_end', 'alaqajat_target', 'alaqajat_izafa', 'alaqajat_kami', 'alaqajat_ikhtitaam',
            'halqajat_start', 'halqajat_end', 'halqajat_target', 'halqajat_izafa', 'halqajat_kami', 'halqajat_ikhtitaam',
            'halqajat_ward_start', 'halqajat_ward_end', 'halqajat_ward_target', 'halqajat_ward_izafa', 'halqajat_ward_kami', 'halqajat_ward_ikhtitaam',
            'block_code_start', 'block_code_end', 'block_code_target', 'block_code_izafa', 'block_code_kami', 'block_code_ikhtitaam',
            'arkaan_start', 'arkaan_end', 'arkaan_target', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
            'umeedwaran_start', 'umeedwaran_end', 'umeedwaran_target', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
            'karkunan_start', 'karkunan_end', 'karkunan_target', 'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam',
            'hangami_start', 'hangami_end', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
            'muawanin_start', 'muawanin_end', 'muawanin_target', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
            'mutayyin_afrad_start', 'mutayyin_afrad_end', 'mutayyin_afrad_target', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
            'member_start', 'member_end', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam',
            'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'youth_programs_json',
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
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
            'nizam_e_fajar_start', 'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
            'awaami_committee_start', 'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
        ]
        
        # Lock fields that are not admin editable
        for field in all_possible_fields:
            locked_fields[field] = field not in admin_editable_fields
        
        # Load youth programs
        programat_list = []
        if report_exists:
            programat_json = form_data.get('youth_programs_json', '')
            if programat_json:
                try:
                    programat_list = json.loads(programat_json)
                except Exception:
                    programat_list = []
        
        # Load atifal programs list from JSON
        atifal_programs_list = []
        if report_exists:
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
            # Lock dynamic atifal fields
            locked_fields[f'atifal_nauyiat_{idx+1}'] = True
            locked_fields[f'atifal_programat_count_{idx+1}'] = True
        
        # Lock dynamic youth program fields
        for idx, prog in enumerate(programat_list):
            locked_fields[f'programat_{idx+1}'] = True
            locked_fields[f'programat_count_{idx+1}'] = True
        
        # Handle POST request for form submission
        if request.method == 'POST':
            new_entry = dict(request.form)
            new_entry['zila'] = zila
            new_entry['timestamp'] = datetime.now().isoformat()
            new_entry['submitted_by'] = session['username']
            new_entry['last_submitted_by'] = session.get('role', 'user')
            
            # Only update admin editable fields
            update_fields = {}
            for key in admin_editable_fields:
                if key in new_entry:
                    update_fields[key] = new_entry[key]
            
            update_fields['zila'] = zila
            update_fields['timestamp'] = new_entry['timestamp']
            update_fields['submitted_by'] = new_entry['submitted_by']
            update_fields['last_submitted_by'] = new_entry['last_submitted_by']
            
            if report_exists:
                update_report_in_db(zila, update_fields)
                flash("ہدف کامیابی سے اپ ڈیٹ ہو گئے!", "success")
            else:
                save_report_to_db(update_fields)
                flash("ہدف کامیابی سے محفوظ ہو گئے!", "success")
            
            # Redirect back to the same page to allow multiple submissions
            return redirect(url_for('admin_target_setting', zila=zila))
        
        return render_template("report.html", 
                             zila=zila, 
                             form_data=form_data,
                             report_exists=report_exists,
                             programat_list=programat_list,
                             atifal_programs_list=atifal_programs_list,
                             locked_fields=locked_fields,
                             can_submit=True,
                             is_admin_target_setting=True,
                             is_view_mode=False)
    except Exception as e:
        import logging
        logging.error("Exception in /admin_target_setting route", exc_info=True)
        flash("ہدف درج کرنے میں خرابی۔", "error")
        return redirect(url_for('dashboard'))

@app.route('/view_combined_report/<zila>')
def view_combined_report(zila):
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash("آپ کو رپورٹ دیکھنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('dashboard'))
        
        # Decode URL if needed
        from urllib.parse import unquote
        zila = unquote(zila)
        
        # Get quarterly comparison data
        q1_form_data, q2_form_data = get_quarterly_comparison_data(zila)
        
        # Use Q2 data as primary data (for backward compatibility)
        form_data = q2_form_data if q2_form_data else q1_form_data
        
        # Prepare quarterly comparison data for Tanzeemi Hayyat charts
        block_code_data = {
            'q1': {
                'hadaf': q2_form_data.get('block_code_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('block_code_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('block_code_target', 0),
                'izafa': q2_form_data.get('block_code_izafa', 0)
            }
        }
        
        nizam_fajar_data = {
            'q1': {
                'hadaf': q2_form_data.get('nizam_e_fajar_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('nizam_e_fajar_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('nizam_e_fajar_target', 0),
                'izafa': q2_form_data.get('nizam_e_fajar_izafa', 0)
            }
        }
        
        awaami_committee_data = {
            'q1': {
                'hadaf': q2_form_data.get('awaami_committee_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('awaami_committee_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('awaami_committee_target', 0),
                'izafa': q2_form_data.get('awaami_committee_izafa', 0)
            }
        }
        
        # Prepare ikhtitam data for Tanzeemi Hayyat charts
        alaqajat_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('alaqajat_target', 0),
                'izafa': q1_form_data.get('alaqajat_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('alaqajat_target', 0),
                'izafa': q2_form_data.get('alaqajat_ikhtitaam', 0)
            }
        }
        
        halqajat_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('halqajat_target', 0),
                'izafa': q1_form_data.get('halqajat_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('halqajat_target', 0),
                'izafa': q2_form_data.get('halqajat_ikhtitaam', 0)
            }
        }
        
        halqajat_ward_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('halqajat_ward_target', 0),
                'izafa': q1_form_data.get('halqajat_ward_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('halqajat_ward_target', 0),
                'izafa': q2_form_data.get('halqajat_ward_ikhtitaam', 0)
            }
        }
        
        block_code_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('block_code_target', 0),
                'izafa': q1_form_data.get('block_code_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('block_code_target', 0),
                'izafa': q2_form_data.get('block_code_ikhtitaam', 0)
            }
        }
        
        nizam_fajar_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('nizam_e_fajar_target', 0),
                'izafa': q1_form_data.get('nizam_e_fajar_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('nizam_e_fajar_target', 0),
                'izafa': q2_form_data.get('nizam_e_fajar_ikhtitaam', 0)
            }
        }
        
        awaami_committee_ikhtitam_data = {
            'q1': {
                'hadaf': q2_form_data.get('awaami_committee_target', 0),
                'izafa': q1_form_data.get('awaami_committee_ikhtitaam', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('awaami_committee_target', 0),
                'izafa': q2_form_data.get('awaami_committee_ikhtitaam', 0)
            }
        }
        
        # Add missing data structures for backward compatibility
        alaqajat_data = {
            'q1': {
                'hadaf': q2_form_data.get('alaqajat_target', 0),
                'izafa': q1_form_data.get('alaqajat_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('alaqajat_target', 0),
                'izafa': q2_form_data.get('alaqajat_izafa', 0)
            }
        }
        
        halqajat_data = {
            'q1': {
                'hadaf': q2_form_data.get('halqajat_target', 0),
                'izafa': q1_form_data.get('halqajat_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('halqajat_target', 0),
                'izafa': q2_form_data.get('halqajat_izafa', 0)
            }
        }
        
        halqajat_ward_data = {
            'q1': {
                'hadaf': q2_form_data.get('halqajat_ward_target', 0),
                'izafa': q1_form_data.get('halqajat_ward_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('halqajat_ward_target', 0),
                'izafa': q2_form_data.get('halqajat_ward_izafa', 0)
            }
        }
        
        # Prepare quarterly comparison data for Afradi Quwat charts
        arkaan_data = {
            'q1': {
                'hadaf': q2_form_data.get('arkaan_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('arkaan_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('arkaan_target', 0),
                'izafa': q2_form_data.get('arkaan_izafa', 0)
            }
        }
        
        umeedwaran_data = {
            'q1': {
                'hadaf': q2_form_data.get('umeedwaran_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('umeedwaran_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('umeedwaran_target', 0),
                'izafa': q2_form_data.get('umeedwaran_izafa', 0)
            }
        }
        
        karkunan_data = {
            'q1': {
                'hadaf': q2_form_data.get('karkunan_target', 0),  # Q2 target used for both quarters
                'izafa': q1_form_data.get('karkunan_izafa', 0)
            },
            'q2': {
                'hadaf': q2_form_data.get('karkunan_target', 0),
                'izafa': q2_form_data.get('karkunan_izafa', 0)
            }
        }
        
        # Prepare quarterly comparison data for Ijtimaat charts
        # Zilai Ijtimaat (individual charts)
        zilai_shura_data = {
            'q1': {
                'planned': q2_form_data.get('zilai_shura_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('zilai_shura_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('zilai_shura_planned', 0),
                'held': q2_form_data.get('zilai_shura_held', 0)
            }
        }
        
        nazm_zila_data = {
            'q1': {
                'planned': q2_form_data.get('nazm_zila_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('nazm_zila_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('nazm_zila_planned', 0),
                'held': q2_form_data.get('nazm_zila_held', 0)
            }
        }
        
        nazimin_alaqajat_data = {
            'q1': {
                'planned': q2_form_data.get('nazimin_alaqajat_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('nazimin_alaqajat_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('nazimin_alaqajat_planned', 0),
                'held': q2_form_data.get('nazimin_alaqajat_held', 0)
            }
        }
        
        zilai_ijtima_arkaan_data = {
            'q1': {
                'planned': q2_form_data.get('zilai_ijtima_arkaan_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('zilai_ijtima_arkaan_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('zilai_ijtima_arkaan_planned', 0),
                'held': q2_form_data.get('zilai_ijtima_arkaan_held', 0)
            }
        }
        
        zilai_ijtima_umeedwaran_data = {
            'q1': {
                'planned': q2_form_data.get('zilai_ijtima_umeedwaran_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('zilai_ijtima_umeedwaran_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('zilai_ijtima_umeedwaran_planned', 0),
                'held': q2_form_data.get('zilai_ijtima_umeedwaran_held', 0)
            }
        }
        
        # Tanzeemi Ijtimaat (individual charts)
        ijtima_arkaan_alaqah_data = {
            'q1': {
                'planned': q2_form_data.get('ijtima_arkaan_alaqah_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('ijtima_arkaan_alaqah_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('ijtima_arkaan_alaqah_planned', 0),
                'held': q2_form_data.get('ijtima_arkaan_alaqah_held', 0)
            }
        }
        
        ijtima_umeedwaran_alaqah_data = {
            'q1': {
                'planned': q2_form_data.get('ijtima_umeedwaran_alaqah_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('ijtima_umeedwaran_alaqah_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('ijtima_umeedwaran_alaqah_planned', 0),
                'held': q2_form_data.get('ijtima_umeedwaran_alaqah_held', 0)
            }
        }
        
        ijtima_karkunan_alaqah_data = {
            'q1': {
                'planned': q2_form_data.get('ijtima_karkunaan_alaqah_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('ijtima_karkunaan_alaqah_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('ijtima_karkunaan_alaqah_planned', 0),
                'held': q2_form_data.get('ijtima_karkunaan_alaqah_held', 0)
            }
        }
        
        ijtima_karkunan_halqajat_data = {
            'q1': {
                'planned': q2_form_data.get('ijtima_karkunaan_halqajat_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('ijtima_karkunaan_halqajat_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('ijtima_karkunaan_halqajat_planned', 0),
                'held': q2_form_data.get('ijtima_karkunaan_halqajat_held', 0)
            }
        }
        
        ijtima_nazimin_halqajat_data = {
            'q1': {
                'planned': q2_form_data.get('ijtima_nazimin_halqajat_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('ijtima_nazimin_halqajat_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('ijtima_nazimin_halqajat_planned', 0),
                'held': q2_form_data.get('ijtima_nazimin_halqajat_held', 0)
            }
        }
        
        # Dawati Ijtimaat (individual charts)
        dars_quran_data = {
            'q1': {
                'planned': q2_form_data.get('dars_quran_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('dars_quran_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('dars_quran_planned', 0),
                'held': q2_form_data.get('dars_quran_held', 0)
            }
        }
        
        dawati_camp_data = {
            'q1': {
                'planned': q2_form_data.get('dawati_camp_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('dawati_camp_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('dawati_camp_planned', 0),
                'held': q2_form_data.get('dawati_camp_held', 0)
            }
        }
        
        gharon_tak_dawat_data = {
            'q1': {
                'planned': q2_form_data.get('gharon_tak_dawat_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('gharon_tak_dawat_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('gharon_tak_dawat_planned', 0),
                'held': q2_form_data.get('gharon_tak_dawat_held', 0)
            }
        }
        
        taqseem_literature_data = {
            'q1': {
                'planned': q2_form_data.get('taqseem_literature_planned', 0),  # Q2 planned used for both quarters
                'held': q1_form_data.get('taqseem_literature_held', 0)
            },
            'q2': {
                'planned': q2_form_data.get('taqseem_literature_planned', 0),
                'held': q2_form_data.get('taqseem_literature_held', 0)
            }
        }
        
        # Haq do Karachi and Koi or Baat text content
        haq_do_karachi_text = form_data.get('haq_do_karachi', '')
        koi_or_bat_text = form_data.get('koi_or_bat', '')
        
        return render_template('combined_report.html', 
                             zila=zila,
                             alaqajat_data=alaqajat_data,
                             alaqajat_ikhtitam_data=alaqajat_ikhtitam_data,
                             halqajat_ikhtitam_data=halqajat_ikhtitam_data,
                             halqajat_ward_ikhtitam_data=halqajat_ward_ikhtitam_data,
                             block_code_ikhtitam_data=block_code_ikhtitam_data,
                             nizam_fajar_ikhtitam_data=nizam_fajar_ikhtitam_data,
                             awaami_committee_ikhtitam_data=awaami_committee_ikhtitam_data,
                             halqajat_data=halqajat_data,
                             halqajat_ward_data=halqajat_ward_data,
                             block_code_data=block_code_data,
                             nizam_fajar_data=nizam_fajar_data,
                             awaami_committee_data=awaami_committee_data,
                             arkaan_data=arkaan_data,
                             umeedwaran_data=umeedwaran_data,
                             karkunan_data=karkunan_data,
                             zilai_shura_data=zilai_shura_data,
                             nazm_zila_data=nazm_zila_data,
                             nazimin_alaqajat_data=nazimin_alaqajat_data,
                             zilai_ijtima_arkaan_data=zilai_ijtima_arkaan_data,
                             zilai_ijtima_umeedwaran_data=zilai_ijtima_umeedwaran_data,
                             ijtima_arkaan_alaqah_data=ijtima_arkaan_alaqah_data,
                             ijtima_umeedwaran_alaqah_data=ijtima_umeedwaran_alaqah_data,
                             ijtima_karkunan_alaqah_data=ijtima_karkunan_alaqah_data,
                             ijtima_karkunan_halqajat_data=ijtima_karkunan_halqajat_data,
                             ijtima_nazimin_halqajat_data=ijtima_nazimin_halqajat_data,
                             dars_quran_data=dars_quran_data,
                             dawati_camp_data=dawati_camp_data,
                             gharon_tak_dawat_data=gharon_tak_dawat_data,
                             taqseem_literature_data=taqseem_literature_data,
                             haq_do_karachi_text=haq_do_karachi_text,
                             koi_or_bat_text=koi_or_bat_text)
                             
    except Exception as e:
        import logging
        logging.error("Exception in /view_combined_report route", exc_info=True)
        flash("Error loading combined report", "error")
        return redirect(url_for('dashboard'))

@app.route('/view_graphs/<zila>')
def view_graphs(zila):
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash("آپ کو گراف دیکھنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('dashboard'))
        
        # Decode URL if needed
        from urllib.parse import unquote
        zila = unquote(zila)
        
        # Load report data
        global report_df
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[report_df['zila'] == zila]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[report_df['zila'].str.strip() == normalized_zila]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[report_df['zila'] == db_zila]
                        logging.info(f"Found partial match: {db_zila} for {normalized_zila}")
                        break
        
        if report_row.empty:
            # No data available, create empty data structures
            form_data = {}
        else:
            form_data = report_row.iloc[0].to_dict()
            form_data = {k: (0 if pd.isna(v) else v) for k, v in form_data.items()}
        
        # Prepare data for Tanzeemi Hayyat charts
        alaqajat_data = {
            'hadaf': form_data.get('alaqajat_target', 0),
            'izafa': form_data.get('alaqajat_izafa', 0)
        }
        
        halqajat_data = {
            'hadaf': form_data.get('halqajat_target', 0),
            'izafa': form_data.get('halqajat_izafa', 0)
        }
        
        halqajat_ward_data = {
            'hadaf': form_data.get('halqajat_ward_target', 0),
            'izafa': form_data.get('halqajat_ward_izafa', 0)
        }
        
        block_code_data = {
            'hadaf': form_data.get('block_code_target', 0),
            'izafa': form_data.get('block_code_izafa', 0)
        }
        
        nizam_fajar_data = {
            'hadaf': form_data.get('nizam_e_fajar_target', 0),
            'izafa': form_data.get('nizam_e_fajar_izafa', 0)
        }
        
        awaami_committee_data = {
            'hadaf': form_data.get('awaami_committee_target', 0),
            'izafa': form_data.get('awaami_committee_izafa', 0)
        }
        
        # Prepare data for Afradi Quwat charts
        arkaan_data = {
            'hadaf': form_data.get('arkaan_target', 0),
            'izafa': form_data.get('arkaan_izafa', 0)
        }
        
        umeedwaran_data = {
            'hadaf': form_data.get('umeedwaran_target', 0),
            'izafa': form_data.get('umeedwaran_izafa', 0)
        }
        
        karkunan_data = {
            'hadaf': form_data.get('karkunan_target', 0),
            'izafa': form_data.get('karkunan_izafa', 0)
        }
        
        # Prepare data for Ijtimaat charts - Individual charts for each type
        # Zilai Ijtimaat (individual charts)
        zilai_shura_data = {
            'planned': form_data.get('zilai_shura_planned', 0),
            'held': form_data.get('zilai_shura_held', 0)
        }
        
        nazm_zila_data = {
            'planned': form_data.get('nazm_zila_planned', 0),
            'held': form_data.get('nazm_zila_held', 0)
        }
        
        nazimin_alaqajat_data = {
            'planned': form_data.get('nazimin_alaqajat_planned', 0),
            'held': form_data.get('nazimin_alaqajat_held', 0)
        }
        
        zilai_ijtima_arkaan_data = {
            'planned': form_data.get('zilai_ijtima_arkaan_planned', 0),
            'held': form_data.get('zilai_ijtima_arkaan_held', 0)
        }
        
        zilai_ijtima_umeedwaran_data = {
            'planned': form_data.get('zilai_ijtima_umeedwaran_planned', 0),
            'held': form_data.get('zilai_ijtima_umeedwaran_held', 0)
        }
        
        # Tanzeemi Ijtimaat (individual charts)
        ijtima_arkaan_alaqah_data = {
            'planned': form_data.get('ijtima_arkaan_alaqah_planned', 0),
            'held': form_data.get('ijtima_arkaan_alaqah_held', 0)
        }
        
        ijtima_umeedwaran_alaqah_data = {
            'planned': form_data.get('ijtima_umeedwaran_alaqah_planned', 0),
            'held': form_data.get('ijtima_umeedwaran_alaqah_held', 0)
        }
        
        ijtima_karkunan_alaqah_data = {
            'planned': form_data.get('ijtima_karkunaan_alaqah_planned', 0),
            'held': form_data.get('ijtima_karkunaan_alaqah_held', 0)
        }
        
        ijtima_karkunan_halqajat_data = {
            'planned': form_data.get('ijtima_karkunaan_halqajat_planned', 0),
            'held': form_data.get('ijtima_karkunaan_halqajat_held', 0)
        }
        
        ijtima_nazimin_halqajat_data = {
            'planned': form_data.get('ijtima_nazimin_halqajat_planned', 0),
            'held': form_data.get('ijtima_nazimin_halqajat_held', 0)
        }
        
        # Dawati Ijtimaat (individual charts)
        dars_quran_data = {
            'planned': form_data.get('dars_quran_planned', 0),
            'held': form_data.get('dars_quran_held', 0)
        }
        
        dawati_camp_data = {
            'planned': form_data.get('dawati_camp_planned', 0),
            'held': form_data.get('dawati_camp_held', 0)
        }
        
        gharon_tak_dawat_data = {
            'planned': form_data.get('gharon_tak_dawat_planned', 0),
            'held': form_data.get('gharon_tak_dawat_held', 0)
        }
        
        taqseem_literature_data = {
            'planned': form_data.get('taqseem_literature_planned', 0),
            'held': form_data.get('taqseem_literature_held', 0)
        }
        
        # Haq do Karachi and Koi or Baat text content
        haq_do_karachi_text = form_data.get('haq_do_karachi', '')
        koi_or_bat_text = form_data.get('koi_or_bat', '')
        
        return render_template('graphs.html', 
                             zila=zila,
                             alaqajat_data=alaqajat_data,
                             halqajat_data=halqajat_data,
                             halqajat_ward_data=halqajat_ward_data,
                             block_code_data=block_code_data,
                             nizam_fajar_data=nizam_fajar_data,
                             awaami_committee_data=awaami_committee_data,
                             arkaan_data=arkaan_data,
                             umeedwaran_data=umeedwaran_data,
                             karkunan_data=karkunan_data,
                             zilai_shura_data=zilai_shura_data,
                             nazm_zila_data=nazm_zila_data,
                             nazimin_alaqajat_data=nazimin_alaqajat_data,
                             zilai_ijtima_arkaan_data=zilai_ijtima_arkaan_data,
                             zilai_ijtima_umeedwaran_data=zilai_ijtima_umeedwaran_data,
                             ijtima_arkaan_alaqah_data=ijtima_arkaan_alaqah_data,
                             ijtima_umeedwaran_alaqah_data=ijtima_umeedwaran_alaqah_data,
                             ijtima_karkunan_alaqah_data=ijtima_karkunan_alaqah_data,
                             ijtima_karkunan_halqajat_data=ijtima_karkunan_halqajat_data,
                             ijtima_nazimin_halqajat_data=ijtima_nazimin_halqajat_data,
                             dars_quran_data=dars_quran_data,
                             dawati_camp_data=dawati_camp_data,
                             gharon_tak_dawat_data=gharon_tak_dawat_data,
                             taqseem_literature_data=taqseem_literature_data,
                             haq_do_karachi_text=haq_do_karachi_text,
                             koi_or_bat_text=koi_or_bat_text)
                             
    except Exception as e:
        import logging
        logging.error("Exception in /view_graphs route", exc_info=True)
        flash("Error loading graphs", "error")
        return redirect(url_for('dashboard'))

@app.route('/toggle_report_lock/<zila>', methods=['POST'])
def toggle_report_lock(zila):
    try:
        if 'username' not in session or session.get('role') != 'admin':
            flash("آپ کو یہ عمل کرنے کی اجازت نہیں ہے", "error")
            return redirect(url_for('dashboard'))
        
        # URL decode the zila name properly
        from urllib.parse import unquote
        decoded_zila = unquote(zila)
        
        # First, ensure the is_locked column exists
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check if is_locked column exists
        c.execute("PRAGMA table_info(monthly_reports)")
        columns = [row[1] for row in c.fetchall()]
        
        if 'is_locked' not in columns:
            # Add the is_locked column if it doesn't exist
            try:
                c.execute("ALTER TABLE monthly_reports ADD COLUMN is_locked INTEGER DEFAULT 0")
                conn.commit()
            except Exception as e:
                # Column might already exist, continue
                pass
        
        # Find the report for this zila in Q2 2025 - try multiple approaches
        c.execute("""
            SELECT is_locked FROM monthly_reports 
            WHERE zila = ? AND period = 'Q2 2025'
        """, (decoded_zila,))
        
        result = c.fetchone()
        
        # If not found, try to find by partial match
        if not result:
            c.execute("""
                SELECT is_locked, zila FROM monthly_reports 
                WHERE period = 'Q2 2025'
            """)
            all_reports = c.fetchall()
            
            # Look for the best match
            best_match = None
            for report in all_reports:
                if decoded_zila == report[1] or decoded_zila in report[1] or report[1] in decoded_zila:
                    best_match = report
                    break
            
            if best_match:
                result = (best_match[0],)
                decoded_zila = best_match[1]  # Use the exact name from database
            else:
                flash(f"رپورٹ نہیں ملی: {decoded_zila}", "error")
                conn.close()
                return redirect(url_for('dashboard'))
        
        # Get current lock status
        current_lock_status = bool(result[0])
        
        # Toggle the lock status
        new_lock_status = not current_lock_status
        
        # Update the lock status using the exact name from database
        c.execute("""
            UPDATE monthly_reports 
            SET is_locked = ? 
            WHERE zila = ? AND period = 'Q2 2025'
        """, (1 if new_lock_status else 0, decoded_zila))
        
        conn.commit()
        conn.close()
        
        # Reload reports
        global report_df
        report_df = load_reports_from_db()
        
        status_text = "لاک" if new_lock_status else "انلاک"
        flash(f"{decoded_zila} کی رپورٹ {status_text} کر دی گئی ہے", "success")
        
    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error in toggle_report_lock for {zila}: {str(e)}")
        flash("خطا: " + str(e), "error")
    
    # Check if this is an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({
            'success': True,
            'message': f"{decoded_zila} کی رپورٹ {status_text} کر دی گئی ہے",
            'new_status': new_lock_status
        })
    
    return redirect(url_for('dashboard'))

@app.route('/logout')
def logout():
    session.clear()
    flash("آپ کامیابی سے لاگ آؤٹ ہو گئے ہیں", "info")
    return redirect(url_for('login'))











@app.route('/spider_graph/<zila>')
def spider_graph(zila):
    try:
        if 'username' not in session:
            return redirect(url_for('login'))
        from urllib.parse import unquote
        zila = unquote(zila)
        global report_df
        report_df = load_reports_from_db()
        # Filter for Q2 2025 data only
        report_row = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q2 2025')]
        if report_row.empty:
            normalized_zila = zila.strip()
            report_row = report_df[(report_df['zila'].str.strip() == normalized_zila) & (report_df['period'] == 'Q2 2025')]
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[(report_df['zila'] == db_zila) & (report_df['period'] == 'Q2 2025')]
                        break
        if report_row.empty:
            form_data = {}
        else:
            form_data = report_row.iloc[0].to_dict()
            form_data = {k: (0 if pd.isna(v) else v) for k, v in form_data.items()}
        block_code_data = {
            'hadaf': form_data.get('block_code_target', 0),
            'izafa': form_data.get('block_code_izafa', 0)
        }
        nizam_fajar_data = {
            'hadaf': form_data.get('nizam_e_fajar_target', 0),
            'izafa': form_data.get('nizam_e_fajar_izafa', 0)
        }
        awaami_committee_data = {
            'hadaf': form_data.get('awaami_committee_target', 0),
            'izafa': form_data.get('awaami_committee_izafa', 0)
        }
        arkaan_data = {
            'hadaf': form_data.get('arkaan_target', 0),
            'izafa': form_data.get('arkaan_izafa', 0)
        }
        umeedwaran_data = {
            'hadaf': form_data.get('umeedwaran_target', 0),
            'izafa': form_data.get('umeedwaran_izafa', 0)
        }
        karkunan_data = {
            'hadaf': form_data.get('karkunan_target', 0),
            'izafa': form_data.get('karkunan_izafa', 0)
        }
        zilai_shura_data = {
            'planned': form_data.get('zilai_shura_planned', 0),
            'held': form_data.get('zilai_shura_held', 0)
        }
        nazm_zila_data = {
            'planned': form_data.get('nazm_zila_planned', 0),
            'held': form_data.get('nazm_zila_held', 0)
        }
        nazimin_alaqajat_data = {
            'planned': form_data.get('nazimin_alaqajat_planned', 0),
            'held': form_data.get('nazimin_alaqajat_held', 0)
        }
        zilai_ijtima_arkaan_data = {
            'planned': form_data.get('zilai_ijtima_arkaan_planned', 0),
            'held': form_data.get('zilai_ijtima_arkaan_held', 0)
        }
        zilai_ijtima_umeedwaran_data = {
            'planned': form_data.get('zilai_ijtima_umeedwaran_planned', 0),
            'held': form_data.get('zilai_ijtima_umeedwaran_held', 0)
        }
        ijtima_arkaan_alaqah_data = {
            'planned': form_data.get('ijtima_arkaan_alaqah_planned', 0),
            'held': form_data.get('ijtima_arkaan_alaqah_held', 0)
        }
        ijtima_umeedwaran_alaqah_data = {
            'planned': form_data.get('ijtima_umeedwaran_alaqah_planned', 0),
            'held': form_data.get('ijtima_umeedwaran_alaqah_held', 0)
        }
        ijtima_karkunan_alaqah_data = {
            'planned': form_data.get('ijtima_karkunaan_alaqah_planned', 0),
            'held': form_data.get('ijtima_karkunaan_alaqah_held', 0)
        }
        ijtima_karkunan_halqajat_data = {
            'planned': form_data.get('ijtima_karkunaan_halqajat_planned', 0),
            'held': form_data.get('ijtima_karkunaan_halqajat_held', 0)
        }
        ijtima_nazimin_halqajat_data = {
            'planned': form_data.get('ijtima_nazimin_halqajat_planned', 0),
            'held': form_data.get('ijtima_nazimin_halqajat_held', 0)
        }
        dars_quran_data = {
            'planned': form_data.get('dars_quran_planned', 0),
            'held': form_data.get('dars_quran_held', 0)
        }
        dawati_camp_data = {
            'planned': form_data.get('dawati_camp_planned', 0),
            'held': form_data.get('dawati_camp_held', 0)
        }
        gharon_tak_dawat_data = {
            'planned': form_data.get('gharon_tak_dawat_planned', 0),
            'held': form_data.get('gharon_tak_dawat_held', 0)
        }
        taqseem_literature_data = {
            'planned': form_data.get('taqseem_literature_planned', 0),
            'held': form_data.get('taqseem_literature_held', 0)
        }
        return render_template('spider_graph.html',
            zila=zila,
            block_code_data=block_code_data,
            nizam_fajar_data=nizam_fajar_data,
            awaami_committee_data=awaami_committee_data,
            arkaan_data=arkaan_data,
            umeedwaran_data=umeedwaran_data,
            karkunan_data=karkunan_data,
            zilai_shura_data=zilai_shura_data,
            nazm_zila_data=nazm_zila_data,
            nazimin_alaqajat_data=nazimin_alaqajat_data,
            zilai_ijtima_arkaan_data=zilai_ijtima_arkaan_data,
            zilai_ijtima_umeedwaran_data=zilai_ijtima_umeedwaran_data,
            ijtima_arkaan_alaqah_data=ijtima_arkaan_alaqah_data,
            ijtima_umeedwaran_alaqah_data=ijtima_umeedwaran_alaqah_data,
            ijtima_karkunan_alaqah_data=ijtima_karkunan_alaqah_data,
            ijtima_karkunan_halqajat_data=ijtima_karkunan_halqajat_data,
            ijtima_nazimin_halqajat_data=ijtima_nazimin_halqajat_data,
            dars_quran_data=dars_quran_data,
            dawati_camp_data=dawati_camp_data,
            gharon_tak_dawat_data=gharon_tak_dawat_data,
            taqseem_literature_data=taqseem_literature_data
        )
    except Exception as e:
        import logging
        logging.error("Exception in /spider_graph route", exc_info=True)
        flash("Error loading spider graph", "error")
        return redirect(url_for('dashboard'))


@app.route('/update_historical_columns')
def update_historical_columns():
    """Route to add historical data columns to database"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get current table structure
        c.execute("PRAGMA table_info(monthly_reports)")
        existing_columns = [row[1] for row in c.fetchall()]
        
        # List of all required columns for historical data
        required_columns = [
            'period', 'submitted_by',
            'alaqajat_start', 'alaqajat_end', 'alaqajat_target', 'alaqajat_izafa', 'alaqajat_kami', 'alaqajat_ikhtitaam',
            'halqajat_start', 'halqajat_end', 'halqajat_target', 'halqajat_izafa', 'halqajat_kami', 'halqajat_ikhtitaam',
            'halqajat_ward_start', 'halqajat_ward_end', 'halqajat_ward_target', 'halqajat_ward_izafa', 'halqajat_ward_kami', 'halqajat_ward_ikhtitaam',
            'block_code_start', 'block_code_end', 'block_code_target', 'block_code_izafa', 'block_code_kami', 'block_code_ikhtitaam',
            'arkaan_start', 'arkaan_end', 'arkaan_target', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
            'umeedwaran_start', 'umeedwaran_end', 'umeedwaran_target', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
            'karkunan_start', 'karkunan_end', 'karkunan_target', 'karkunan_izafa', 'karkunan_kami', 'karkunan_ikhtitaam',
            'hangami_start', 'hangami_end', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
            'muawanin_start', 'muawanin_end', 'muawanin_target', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
            'mutayyin_afrad_start', 'mutayyin_afrad_end', 'mutayyin_afrad_target', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
            'member_start', 'member_end', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam',
            'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count', 'youth_programs_json',
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
            'nizam_e_fajar_start', 'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
            'awaami_committee_start', 'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
            'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json'
        ]
        
        # Add dynamic program fields (up to 50 for each type)
        for i in range(1, 51):
            required_columns.extend([
                f'programat_{i}',
                f'programat_count_{i}',
                f'atifal_programat_{i}',
                f'atifal_programat_count_{i}'
            ])
        
        messages = []
        
        # Add missing columns
        for column in required_columns:
            if column not in existing_columns:
                try:
                    # Text fields should be TEXT, others should be INTEGER
                    if 'programat_' in column and not column.endswith('_count'):
                        c.execute(f"ALTER TABLE monthly_reports ADD COLUMN {column} TEXT DEFAULT ''")
                    else:
                        c.execute(f"ALTER TABLE monthly_reports ADD COLUMN {column} INTEGER DEFAULT 0")
                    messages.append(f"Column '{column}' added successfully.")
                except Exception as e:
                    messages.append(f"Error adding column '{column}': {str(e)}")
            else:
                messages.append(f"Column '{column}' already exists.")
        
        # Remove the UNIQUE constraint on zila to allow multiple records per zila
        try:
            # Check if UNIQUE constraint exists
            c.execute("PRAGMA index_list(monthly_reports)")
            indexes = c.fetchall()
            
            # Look for UNIQUE constraint on zila
            unique_constraint_exists = False
            for index in indexes:
                if 'zila' in str(index) and index[2] == 1:  # index[2] is unique flag
                    unique_constraint_exists = True
                    break
            
            if unique_constraint_exists:
                # Create a new table without the UNIQUE constraint
                c.execute('''
                    CREATE TABLE monthly_reports_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        zila TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        submitted_by TEXT NOT NULL,
                        last_submitted_by TEXT,
                        period TEXT
                    )
                ''')
                
                # Copy all data from old table to new table
                c.execute('''
                    INSERT INTO monthly_reports_new 
                    SELECT id, zila, timestamp, submitted_by, last_submitted_by, period FROM monthly_reports
                ''')
                
                # Drop the old table
                c.execute('DROP TABLE monthly_reports')
                
                # Rename new table to original name
                c.execute('ALTER TABLE monthly_reports_new RENAME TO monthly_reports')
                
                # Re-add all the columns
                for column in required_columns:
                    if column not in ['period', 'submitted_by']:  # These are already in the new table
                        try:
                            c.execute(f"ALTER TABLE monthly_reports ADD COLUMN {column} INTEGER DEFAULT 0")
                        except:
                            pass
                
                messages.append("UNIQUE constraint on zila removed successfully.")
            else:
                messages.append("No UNIQUE constraint found on zila.")
                
        except Exception as e:
            messages.append(f"Error removing UNIQUE constraint: {str(e)}")
        
        conn.commit()
        conn.close()
        
        return "<br>".join(messages)
        
    except Exception as e:
        return f"Error updating columns: {str(e)}"












@app.route('/historical_data_form/<zila>', methods=['GET'])
def historical_data_form(zila):
    """Route for viewing existing historical data and providing access to submit new data"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    # Check if user is an agent or admin
    if session.get('role') not in ['agent', 'admin']:
        flash("صرف ایجنٹ اور ایڈمن صارفین تاریخی ڈیٹا دیکھ سکتے ہیں", "error")
        return redirect(url_for('dashboard'))
    
    # Decode URL if needed
    from urllib.parse import unquote
    zila = unquote(zila)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Get existing historical submissions for this zila
    c.execute('''
        SELECT period, submitted_by, timestamp 
        FROM monthly_reports 
        WHERE zila = ? AND period IS NOT NULL AND period != 'None None'
        ORDER BY timestamp DESC
    ''', (zila,))
    existing_submissions = c.fetchall()
    
    conn.close()
    
    return render_template('historical_data_form.html', 
                         zila=zila, 
                         existing_submissions=existing_submissions,
                         username=session['username'])

@app.route('/submit_historical_data/<zila>', methods=['GET', 'POST'])
def submit_historical_data(zila):
    """Route for agents and admins to submit new historical quarterly data"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    # Check if user is an agent or admin
    if session.get('role') not in ['agent', 'admin']:
        flash("صرف ایجنٹ اور ایڈمن صارفین تاریخی ڈیٹا جمع کر سکتے ہیں", "error")
        return redirect(url_for('dashboard'))
    
    # Decode URL if needed
    from urllib.parse import unquote
    zila = unquote(zila)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    if request.method == 'GET':
        # Check if editing a specific period
        period_to_edit = request.args.get('edit_period')
        
        if period_to_edit:
            # Get the specific record to edit
            c.execute('''
                SELECT * FROM monthly_reports 
                WHERE zila = ? AND period = ?
            ''', (zila, period_to_edit))
            record_to_edit = c.fetchone()
            
            if record_to_edit:
                # Get column names
                c.execute("PRAGMA table_info(monthly_reports)")
                columns = [row[1] for row in c.fetchall()]
                
                # Create a dictionary of the record data
                record_data = dict(zip(columns, record_to_edit))
                
                conn.close()
                
                # Convert record_data to form_data format for template
                form_data = record_data
                
                # Process youth programs from JSON and populate individual fields
                youth_programs_json = form_data.get('youth_programs_json', '')
                if youth_programs_json:
                    try:
                        youth_programs = json.loads(youth_programs_json)
                        # Set the count to the actual number of programs in JSON
                        form_data['youth_programat_count'] = str(len(youth_programs))
                        for idx, prog in enumerate(youth_programs):
                            form_data[f'programat_{idx+1}'] = prog.get('name', '')
                            form_data[f'programat_count_{idx+1}'] = prog.get('count', '')
                    except Exception as e:
                        # If JSON parsing fails, fall back to individual fields
                        for i in range(1, 51):
                            program_name = record_data.get(f'programat_{i}', '')
                            program_count = record_data.get(f'programat_count_{i}', '')
                            form_data[f'programat_{i}'] = '' if pd.isna(program_name) else str(program_name)
                            form_data[f'programat_count_{i}'] = '' if pd.isna(program_count) else str(program_count)
                else:
                    # No JSON data, load from individual fields
                    for i in range(1, 51):
                        program_name = record_data.get(f'programat_{i}', '')
                        program_count = record_data.get(f'programat_count_{i}', '')
                        form_data[f'programat_{i}'] = '' if pd.isna(program_name) else str(program_name)
                        form_data[f'programat_count_{i}'] = '' if pd.isna(program_count) else str(program_count)
                
                # Process atifal programs from JSON and populate individual fields
                atifal_programs_json = form_data.get('atifal_programs_json', '')
                if atifal_programs_json:
                    try:
                        atifal_programs = json.loads(atifal_programs_json)
                        # Set the count to the actual number of programs in JSON
                        form_data['atifal_programat_count'] = str(len(atifal_programs))
                        for idx, prog in enumerate(atifal_programs):
                            form_data[f'atifal_nauyiat_{idx+1}'] = prog.get('nauiyat', '')
                            form_data[f'atifal_programat_count_{idx+1}'] = prog.get('hazri', '')
                    except Exception as e:
                        # If JSON parsing fails, fall back to individual fields
                        for i in range(1, 51):
                            atifal_nauiyat = record_data.get(f'atifal_nauyiat_{i}', '')
                            atifal_hazri = record_data.get(f'atifal_programat_count_{i}', '')
                            form_data[f'atifal_nauyiat_{i}'] = '' if pd.isna(atifal_nauiyat) else str(atifal_nauiyat)
                            form_data[f'atifal_programat_count_{i}'] = '' if pd.isna(atifal_hazri) else str(atifal_hazri)
                else:
                    # No JSON data, load from individual fields
                    for i in range(1, 51):
                        atifal_nauiyat = record_data.get(f'atifal_nauyiat_{i}', '')
                        atifal_hazri = record_data.get(f'atifal_programat_count_{i}', '')
                        form_data[f'atifal_nauyiat_{i}'] = '' if pd.isna(atifal_nauiyat) else str(atifal_nauiyat)
                        form_data[f'atifal_programat_count_{i}'] = '' if pd.isna(atifal_hazri) else str(atifal_hazri)
                
                # Extract quarter and year from period for the dropdowns
                if record_data.get('period'):
                    period_parts = record_data['period'].split()
                    if len(period_parts) == 2:
                        form_data['quarter'] = period_parts[0]
                        form_data['year'] = period_parts[1]
                
                return render_template('submit_historical_data.html', 
                                     zila=zila, 
                                     username=session['username'],
                                     form_data=form_data,
                                     editing=True,
                                     edit_period=period_to_edit,
                                     time=datetime.now().timestamp())
        
        # Get the previous quarter to fill in
        previous_quarter, previous_year = get_previous_quarter_for_zila(zila)
        
        # Get the next quarter's ikhtitam values to use as aghaz
        next_ikhtitam_values = get_next_quarter_ikhtitam(zila, previous_quarter, int(previous_year))
        
        # Create form_data with the suggested quarter and year
        form_data = {
            'quarter': previous_quarter,
            'year': previous_year
        }
        
        # Add next quarter's ikhtitam values as aghaz
        form_data.update(next_ikhtitam_values)
        
        conn.close()
        
        return render_template('submit_historical_data.html', 
                             zila=zila, 
                             username=session['username'],
                             form_data=form_data,
                             time=datetime.now().timestamp())
    
    elif request.method == 'POST':
        try:
            # Initialize database connection
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            
            # Get form data
            quarter = request.form.get('quarter')
            year = request.form.get('year')
            
            # If quarter/year not provided, get the previous suggested quarter
            if not quarter or not year:
                previous_quarter, previous_year = get_previous_quarter_for_zila(zila)
                quarter = quarter or previous_quarter
                year = year or previous_year
            
            period = f"{quarter} {year}"
            
            # Create new entry with historical data
            new_entry = {
                'zila': zila,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'submitted_by': session['username'],
                'period': period
            }
            
            # Get existing columns from database
            c.execute("PRAGMA table_info(monthly_reports)")
            existing_columns = [row[1] for row in c.fetchall()]
            
            # Add all form fields (similar to the main report form)
            all_form_fields = [
                'union_committee_count', 'wards_count', 'block_code_count', 'cantonment_board_count',
                'nazm_qaim_union', 'nazm_qaim_wards', 'nazm_qaim_blockcode', 'nazm_qaim_cantonment',
                'alaqajat_start', 'alaqajat_end', 'alaqajat_target', 'alaqajat_izafa', 'alaqajat_kami', 'alaqajat_ikhtitaam',
                'halqajat_start', 'halqajat_end', 'halqajat_target', 'halqajat_izafa', 'halqajat_kami', 'halqajat_ikhtitaam',
                'halqajat_ward_start', 'halqajat_ward_end', 'halqajat_ward_target', 'halqajat_ward_izafa', 'halqajat_ward_kami', 'halqajat_ward_ikhtitaam',
                'block_code_start', 'block_code_end', 'block_code_target', 'block_code_izafa', 'block_code_kami', 'block_code_ikhtitaam',
                'arkaan_start', 'arkaan_end', 'arkaan_target', 'arkaan_izafa', 'arkaan_kami', 'arkaan_ikhtitaam',
                'umeedwaran_start', 'umeedwaran_end', 'umeedwaran_target', 'umeedwaran_izafa', 'umeedwaran_kami', 'umeedwaran_ikhtitaam',
                'hangami_start', 'hangami_end', 'hangami_target', 'hangami_izafa', 'hangami_kami', 'hangami_ikhtitaam',
                'muawanin_start', 'muawanin_end', 'muawanin_target', 'muawanin_izafa', 'muawanin_kami', 'muawanin_ikhtitaam',
                'mutayyin_afrad_start', 'mutayyin_afrad_end', 'mutayyin_afrad_target', 'mutayyin_afrad_izafa', 'mutayyin_afrad_kami', 'mutayyin_afrad_ikhtitaam',
                'member_start', 'member_end', 'member_target', 'member_izafa', 'member_kami', 'member_ikhtitaam',
                'youth_nazm_areas', 'youth_karkunan', 'youth_programat_count',
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
                'nizam_e_fajar_start', 'nizam_e_fajar_target', 'nizam_e_fajar_izafa', 'nizam_e_fajar_kami', 'nizam_e_fajar_ikhtitaam',
                'awaami_committee_start', 'awaami_committee_target', 'awaami_committee_izafa', 'awaami_committee_kami', 'awaami_committee_ikhtitaam',
                'atifal_nazm_areas', 'atifal_members', 'atifal_programat_count', 'atifal_programs_json',
            ]
            
            # Only use fields that exist in the database
            form_fields = [field for field in all_form_fields if field in existing_columns]
            
            # Define text fields that should not be converted to integers
            text_fields = ['koi_or_bat', 'haq_do_karachi', 'other_trainings']
            
            for field in form_fields:
                value = request.form.get(field, '')
                if field in text_fields:
                    # Handle text fields - store as string
                    new_entry[field] = value
                else:
                    # Handle numeric fields
                    if value == '':
                        new_entry[field] = 0
                    else:
                        try:
                            new_entry[field] = int(value)
                        except ValueError:
                            new_entry[field] = 0
            
            # Handle dynamic youth programs (convert to JSON)
            youth_programat_count = request.form.get('youth_programat_count', '0')
            try:
                youth_programat_count = int(youth_programat_count)
            except ValueError:
                youth_programat_count = 0
            
            
            youth_programs = []
            for i in range(1, youth_programat_count + 1):
                programat_name = request.form.get(f'programat_{i}', '').strip()
                programat_count = request.form.get(f'programat_count_{i}', '0')
                try:
                    programat_count = int(programat_count)
                except ValueError:
                    programat_count = 0
                
                if programat_name:  # Only add if name is not empty
                    youth_programs.append({
                        'name': programat_name,
                        'count': str(programat_count)
                    })
            
            
            
            # Save youth programs as JSON
            if 'youth_programs_json' in existing_columns:
                new_entry['youth_programs_json'] = json.dumps(youth_programs, ensure_ascii=False)
                new_entry['youth_programat_count'] = youth_programat_count
            
            # Handle dynamic atifal programs (convert to JSON)
            atifal_programat_count = request.form.get('atifal_programat_count', '0')
            try:
                atifal_programat_count = int(atifal_programat_count)
            except ValueError:
                atifal_programat_count = 0
            
            atifal_programs = []
            for i in range(1, atifal_programat_count + 1):
                atifal_nauiyat = request.form.get(f'atifal_nauyiat_{i}', '').strip()
                atifal_hazri = request.form.get(f'atifal_programat_count_{i}', '0')
                try:
                    atifal_hazri = int(atifal_hazri)
                except ValueError:
                    atifal_hazri = 0
                
                if atifal_nauiyat:  # Only add if name is not empty
                    atifal_programs.append({
                        'nauiyat': atifal_nauiyat,
                        'hazri': atifal_hazri
                    })
            
            # Save atifal programs as JSON
            if 'atifal_programs_json' in existing_columns:
                new_entry['atifal_programs_json'] = json.dumps(atifal_programs, ensure_ascii=False)
                new_entry['atifal_programat_count'] = atifal_programat_count
            
            # Check if this period already exists for this zila
            c.execute('SELECT id FROM monthly_reports WHERE zila = ? AND period = ?', (zila, period))
            existing = c.fetchone()
            
            if existing:
                # Update existing record with all form fields
                update_fields = []
                update_values = []
                
                for field in form_fields:
                    if field in new_entry:
                        update_fields.append(f"{field} = ?")
                        update_values.append(new_entry[field])
                
                update_fields.extend(['timestamp = ?', 'submitted_by = ?'])
                update_values.extend([new_entry['timestamp'], new_entry['submitted_by']])
                update_values.extend([zila, period])
                
                update_query = f'''
                    UPDATE monthly_reports SET 
                    {', '.join(update_fields)}
                    WHERE zila = ? AND period = ?
                '''
                c.execute(update_query, update_values)
                flash(f"{period} کے لیے ڈیٹا کامیابی سے اپ ڈیٹ ہو گیا", "success")
            else:
                # Insert new record
                columns = ', '.join(new_entry.keys())
                placeholders = ', '.join(['?' for _ in new_entry])
                insert_query = f'INSERT INTO monthly_reports ({columns}) VALUES ({placeholders})'
                try:
                    c.execute(insert_query, list(new_entry.values()))
                except Exception as insert_error:
                    raise insert_error
                flash(f"{period} کے لیے ڈیٹا کامیابی سے جمع ہو گیا", "success")
            
            conn.commit()
            conn.close()
            
            return redirect(url_for('historical_data_form', zila=zila))
            
        except Exception as e:
            conn.close()
            import traceback
            error_details = traceback.format_exc()
            flash(f"ڈیٹا جمع کرنے میں خرابی: {str(e)}<br><br>Details: {error_details}", "error")
            return redirect(url_for('historical_data_form', zila=zila))



@app.route('/add_missing_columns')
def add_missing_columns():
    """Add missing JSON columns to database"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get existing columns
        c.execute('PRAGMA table_info(monthly_reports)')
        existing_columns = [row[1] for row in c.fetchall()]
        
        print(f"Existing columns: {existing_columns}")
        
        # Add missing JSON columns
        missing_columns = []
        
        if 'youth_programs_json' not in existing_columns:
            c.execute('ALTER TABLE monthly_reports ADD COLUMN youth_programs_json TEXT')
            missing_columns.append('youth_programs_json')
            print("Added youth_programs_json column")
        else:
            print("youth_programs_json column already exists")
        
        if 'atifal_programs_json' not in existing_columns:
            c.execute('ALTER TABLE monthly_reports ADD COLUMN atifal_programs_json TEXT')
            missing_columns.append('atifal_programs_json')
            print("Added atifal_programs_json column")
        else:
            print("atifal_programs_json column already exists")
        
        # Add missing text columns
        text_columns = ['koi_or_bat', 'haq_do_karachi', 'other_trainings']
        for col in text_columns:
            if col not in existing_columns:
                c.execute(f'ALTER TABLE monthly_reports ADD COLUMN {col} TEXT')
                missing_columns.append(col)
                print(f"Added {col} column")
            else:
                print(f"{col} column already exists")
        
        conn.commit()
        conn.close()
        
        if missing_columns:
            flash(f"Added missing columns: {', '.join(missing_columns)}", "success")
        else:
            flash("All required columns already exist", "info")
        
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        conn.close()
        flash(f"Error adding columns: {str(e)}", "error")
        return redirect(url_for('dashboard'))




@app.route('/test_json_saving')
def test_json_saving():
    """Test JSON saving functionality"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get all columns
        c.execute('PRAGMA table_info(monthly_reports)')
        all_columns = [row[1] for row in c.fetchall()]
        
        # Check for JSON columns
        youth_json_exists = 'youth_programs_json' in all_columns
        atifal_json_exists = 'atifal_programs_json' in all_columns
        
        # Test JSON creation
        test_youth_programs = [
            {'nauiyat': 'Test Youth Program 1', 'hazri': 10},
            {'nauiyat': 'Test Youth Program 2', 'hazri': 15}
        ]
        
        test_atifal_programs = [
            {'nauiyat': 'Test Atifal Program 1', 'hazri': 5},
            {'nauiyat': 'Test Atifal Program 2', 'hazri': 8}
        ]
        
        youth_json = json.dumps(test_youth_programs, ensure_ascii=False)
        atifal_json = json.dumps(test_atifal_programs, ensure_ascii=False)
        
        result = {
            'all_columns': all_columns,
            'youth_json_exists': youth_json_exists,
            'atifal_json_exists': atifal_json_exists,
            'test_youth_json': youth_json,
            'test_atifal_json': atifal_json,
            'youth_json_length': len(youth_json),
            'atifal_json_length': len(atifal_json)
        }
        
        conn.close()
        return jsonify(result)
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)})

@app.route('/test_youth_form')
def test_youth_form():
    """Test youth form submission"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    # Simulate form data
    test_form_data = {
        'youth_programat_count': '2',
        'programat_1': 'Test Program 1',
        'programat_count_1': '10',
        'programat_2': 'Test Program 2',
        'programat_count_2': '15'
    }
    
    # Process the test data
    youth_programat_count = int(test_form_data.get('youth_programat_count', '0'))
    youth_programs = []
    
    for i in range(1, youth_programat_count + 1):
        programat_name = test_form_data.get(f'programat_{i}', '').strip()
        programat_count = int(test_form_data.get(f'programat_count_{i}', '0'))
        
        if programat_name:
            youth_programs.append({
                'name': programat_name,
                'count': str(programat_count)
            })
    
    youth_json = json.dumps(youth_programs, ensure_ascii=False)
    
    result = {
        'test_form_data': test_form_data,
        'youth_programat_count': youth_programat_count,
        'youth_programs': youth_programs,
        'youth_json': youth_json,
        'youth_json_length': len(youth_json)
    }
    
    return jsonify(result)

@app.route('/debug_periods/<zila>')
def debug_periods(zila):
    """Debug route to check what periods exist for a zila"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get all records for this zila
        c.execute('SELECT zila, period, submitted_by, timestamp FROM monthly_reports WHERE zila = ? ORDER BY timestamp DESC', (zila,))
        records = c.fetchall()
        
        # Get all unique periods in the database
        c.execute('SELECT DISTINCT period FROM monthly_reports WHERE period IS NOT NULL ORDER BY period')
        all_periods = [row[0] for row in c.fetchall()]
        
        # Get records without period
        c.execute('SELECT zila, period, submitted_by, timestamp FROM monthly_reports WHERE zila = ? AND (period IS NULL OR period = "")', (zila,))
        null_periods = c.fetchall()
        
        conn.close()
        
        debug_info = {
            'zila': zila,
            'all_records': records,
            'all_periods_in_db': all_periods,
            'null_period_records': null_periods,
            'total_records_for_zila': len(records)
        }
        
        return f"""
        <h2>Debug Info for {zila}</h2>
        <h3>All Records for this Zila:</h3>
        <pre>{records}</pre>
        
        <h3>All Periods in Database:</h3>
        <pre>{all_periods}</pre>
        
        <h3>Records with Null/Empty Period:</h3>
        <pre>{null_periods}</pre>
        
        <h3>Total Records for {zila}: {len(records)}</h3>
        
        <p><a href="/dashboard">Back to Dashboard</a></p>
        """
        
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/fix_invalid_periods')
def fix_invalid_periods():
    """Fix invalid period values in the database"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Find records with invalid period values
        c.execute("SELECT COUNT(*) FROM monthly_reports WHERE period = 'None None' OR period = 'None' OR period = '' OR period IS NULL")
        invalid_count = c.fetchone()[0]
        
        if invalid_count > 0:
            # Update all invalid periods to Q2 2025 (default for existing data)
            c.execute("UPDATE monthly_reports SET period = 'Q2 2025' WHERE period = 'None None' OR period = 'None' OR period = '' OR period IS NULL")
            updated_count = c.rowcount
            conn.commit()
            
            flash(f"Fixed {updated_count} records with invalid period values. Set to Q2 2025.", "success")
        else:
            flash("No invalid period values found.", "info")
        
        conn.close()
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        flash(f"Error fixing periods: {str(e)}", "error")
        return redirect(url_for('dashboard'))

@app.route('/debug_json_loading/<zila>')
def debug_json_loading(zila):
    """Debug route to test JSON loading for a specific zila"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    if session.get('role') != 'admin':
        flash("Access denied", "error")
        return redirect(url_for('dashboard'))
    
    try:
        # Get form data
        form_data = get_form_data(zila)
        
        # Test youth programs JSON
        youth_programs_json = form_data.get('youth_programs_json', '')
        youth_programs_list = []
        if youth_programs_json:
            try:
                youth_programs_list = json.loads(youth_programs_json)
            except Exception as e:
                youth_programs_list = f"JSON Parse Error: {str(e)}"
        
        # Test atifal programs JSON
        atifal_programs_json = form_data.get('atifal_programs_json', '')
        atifal_programs_list = []
        if atifal_programs_json:
            try:
                atifal_programs_list = json.loads(atifal_programs_json)
            except Exception as e:
                atifal_programs_list = f"JSON Parse Error: {str(e)}"
        
        # Check individual fields
        youth_fields = {}
        atifal_fields = {}
        
        for i in range(1, 6):  # Check first 5 fields
            youth_fields[f'programat_{i}'] = form_data.get(f'programat_{i}', 'NOT_FOUND')
            youth_fields[f'programat_count_{i}'] = form_data.get(f'programat_count_{i}', 'NOT_FOUND')
            atifal_fields[f'atifal_nauyiat_{i}'] = form_data.get(f'atifal_nauyiat_{i}', 'NOT_FOUND')
            atifal_fields[f'atifal_programat_count_{i}'] = form_data.get(f'atifal_programat_count_{i}', 'NOT_FOUND')
        
        debug_info = {
            'zila': zila,
            'youth_programs_json_raw': youth_programs_json,
            'youth_programs_parsed': youth_programs_list,
            'atifal_programs_json_raw': atifal_programs_json,
            'atifal_programs_parsed': atifal_programs_list,
            'youth_programat_count': form_data.get('youth_programat_count', 'NOT_FOUND'),
            'atifal_programat_count': form_data.get('atifal_programat_count', 'NOT_FOUND'),
            'youth_individual_fields': youth_fields,
            'atifal_individual_fields': atifal_fields
        }
        
        return f"""
        <h2>JSON Loading Debug for {zila}</h2>
        
        <h3>Youth Programs:</h3>
        <p><strong>Count:</strong> {debug_info['youth_programat_count']}</p>
        <p><strong>Raw JSON:</strong> {debug_info['youth_programs_json_raw']}</p>
        <p><strong>Parsed JSON:</strong> {debug_info['youth_programs_parsed']}</p>
        
        <h3>Atifal Programs:</h3>
        <p><strong>Count:</strong> {debug_info['atifal_programat_count']}</p>
        <p><strong>Raw JSON:</strong> {debug_info['atifal_programs_json_raw']}</p>
        <p><strong>Parsed JSON:</strong> {debug_info['atifal_programs_parsed']}</p>
        
        <h3>Individual Youth Fields (First 5):</h3>
        <pre>{debug_info['youth_individual_fields']}</pre>
        
        <h3>Individual Atifal Fields (First 5):</h3>
        <pre>{debug_info['atifal_individual_fields']}</pre>
        
        <p><a href="/dashboard">Back to Dashboard</a></p>
        """
        
    except Exception as e:
        return f"Error: {str(e)}"

def get_q1_2025_data(zila):
    """Helper function to get Q1 2025 data for a zila"""
    try:
        global report_df
        report_df = load_reports_from_db()
        
        # Try exact match first
        report_row = report_df[(report_df['zila'] == zila) & (report_df['period'] == 'Q1 2025')]
        
        # If no exact match, try case-insensitive and normalized matching
        if report_row.empty:
            # Normalize the zila name (remove extra spaces, etc.)
            normalized_zila = zila.strip()
            report_row = report_df[(report_df['zila'].str.strip() == normalized_zila) & (report_df['period'] == 'Q1 2025')]
            
            # If still no match, try partial matching
            if report_row.empty:
                for db_zila in report_df['zila'].unique():
                    if db_zila and (normalized_zila in db_zila or db_zila in normalized_zila):
                        report_row = report_df[(report_df['zila'] == db_zila) & (report_df['period'] == 'Q1 2025')]
                        break
        
        return report_row
    except Exception as e:
        import logging
        logging.error(f"Error getting Q1 2025 data: {e}", exc_info=True)
        return pd.DataFrame()

def get_quarterly_comparison_data(zila):
    """Get data for both Q1 and Q2 2025 for quarterly comparison"""
    try:
        q1_data = get_q1_2025_data(zila)
        q2_data = get_q2_2025_data(zila)
        
        # Initialize empty data structures
        q1_form_data = {}
        q2_form_data = {}
        
        if not q1_data.empty:
            q1_form_data = q1_data.iloc[0].to_dict()
            q1_form_data = {k: (0 if pd.isna(v) else v) for k, v in q1_form_data.items()}
        
        if not q2_data.empty:
            q2_form_data = q2_data.iloc[0].to_dict()
            q2_form_data = {k: (0 if pd.isna(v) else v) for k, v in q2_form_data.items()}
        
        return q1_form_data, q2_form_data
    except Exception as e:
        import logging
        logging.error(f"Error getting quarterly comparison data: {e}", exc_info=True)
        return {}, {}

if __name__ == '__main__':
    app.run(debug=True, port=5001)