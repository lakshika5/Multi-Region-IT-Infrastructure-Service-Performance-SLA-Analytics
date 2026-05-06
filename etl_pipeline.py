# File: src/etl_pipeline.py
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime, timedelta
import os
import json
import warnings
warnings.filterwarnings('ignore')

class ITInfraETL:
    def __init__(self, base_path):
        self.base_path = base_path
        self.config_path = os.path.join(base_path, "etl_config.json")
        self.conn = None
        self.cursor = None
        self.load_config()
        
    def load_config(self):
        """Load ETL configuration from JSON file"""
        with open(self.config_path, 'r') as f:
            self.config = json.load(f)
        print("ETL configuration loaded")
        
    def connect_to_sql(self):
        """Establish connection to SQL Server"""
        try:
            self.conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=localhost\\SQLEXPRESS;'
                'DATABASE=ITInfraSLAAnalytics;'
                'Trusted_Connection=yes;'
            )
            self.cursor = self.conn.cursor()
            print("Connected to SQL Server successfully")
            return True
        except Exception as e:
            print(f"Failed to connect to SQL Server: {e}")
            return False
            
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")
        
    def load_dimension_tables(self):
        """Load dimension tables from generated CSV files"""
        print("\n" + "="*60)
        print("LOADING DIMENSION TABLES")
        print("="*60)
        
        dimensions = self.config["dimensions"]
        
        for table_name, table_config in dimensions.items():
            try:
                source_file = os.path.join(self.base_path, table_config["source_file"])
                
                if not os.path.exists(source_file):
                    print(f"File not found: {source_file}")
                    continue
                    
                df = pd.read_csv(source_file)
                print(f"Loading {table_name} ({len(df)} rows)...")
                
                # Clear existing data
                self.cursor.execute(f"DELETE FROM {table_name}")
                
                # Insert data
                for _, row in df.iterrows():
                    columns = ', '.join(table_config["columns"])
                    placeholders = ', '.join(['?' for _ in table_config["columns"]])
                    values = [row[col] for col in table_config["columns"]]
                    
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    self.cursor.execute(sql, values)
                
                self.conn.commit()
                print(f"Successfully loaded {len(df)} rows into {table_name}")
                
            except Exception as e:
                print(f"Error loading {table_name}: {e}")
                self.conn.rollback()
                
    def transform_ticket_data(self, df):
        """Transform ticket data to match schema"""
        # Map priority names
        priority_map = self.config["business_rules"]["priority_mapping"]
        df['priority'] = df['priority'].astype(str).map(priority_map).fillna('Medium')
        
        # Clean survey results
        if 'survey_results' in df.columns:
            df['survey_results'] = df['survey_results'].fillna('Neutral')
        
        # Generate ticket_id if not present
        if 'ticket_id' not in df.columns:
            df['ticket_id'] = range(10000, 10000 + len(df))
        
        # Add required columns with default values
        defaults = self.config["facts"]["fact_ticket"]["default_values"]
        for col, default_val in defaults.items():
            if col not in df.columns:
                df[col] = default_val
        
        return df
    
    def transform_incident_data(self, df):
        """Transform incident data to match schema"""
        # Clean incident_state
        if 'incident_state' in df.columns:
            df['incident_state'] = df['incident_state'].astype(str).str.strip().str.lower()
            df['incident_state'] = df['incident_state'].replace({
                'nat': 'new',
                '7': 'resolved',
                '6': 'closed'
            })
        
        # Convert boolean columns
        bool_cols = ['active', 'made_sla']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)
        
        # Parse datetime columns
        date_cols = ['opened_at', 'acknowledged_at', 'resolved_at', 'closed_at']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Add required columns with default values
        defaults = self.config["facts"]["fact_incident"]["default_values"]
        for col, default_val in defaults.items():
            if col not in df.columns:
                df[col] = default_val
        
        return df
    
    def load_fact_tickets(self, sample_size=None):
        """Load ticket data into fact_ticket table"""
        print("\n" + "="*60)
        print("LOADING TICKET DATA")
        print("="*60)
        
        try:
            source_file = os.path.join(self.base_path, "cleaned_customer_tickets.csv")
            df = pd.read_csv(source_file)
            
            if sample_size:
                df = df.head(sample_size)
                print(f"Loading sample of {sample_size} tickets...")
            else:
                print(f"Loading {len(df)} tickets...")
            
            # Transform data
            df = self.transform_ticket_data(df)
            
            # Get dimension mappings
            self.cursor.execute("SELECT agent_id, assignment_group FROM dim_agent")
            agent_map = {row[1]: row[0] for row in self.cursor.fetchall()}
            
            self.cursor.execute("SELECT priority_id, priority_name FROM dim_priority")
            priority_map = {row[1]: row[0] for row in self.cursor.fetchall()}
            
            # Insert tickets
            inserted = 0
            for _, row in df.iterrows():
                try:
                    # Map to dimension keys
                    agent_id = agent_map.get(row.get('queue', 'Network Ops'), 1)
                    priority_id = priority_map.get(row.get('priority', 'Medium'), 3)
                    
                    sql = """
                    INSERT INTO fact_ticket 
                    (ticket_id, client_id, site_id, agent_id, priority_id, 
                     created_at, first_response_at, resolved_at, closed_at,
                     status, source, topic, subject, body, 
                     expected_sla_resolve_min, sla_first_response_min, sla_resolution_min,
                     agent_interactions, survey_results, product_group, support_level)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        int(row.get('ticket_id', 10000 + inserted)),
                        int(row.get('client_id', 1)),
                        int(row.get('site_id', 1)),
                        int(agent_id),
                        int(priority_id),
                        datetime.now() - timedelta(days=np.random.randint(1, 90)),  # created_at
                        datetime.now() - timedelta(days=np.random.randint(1, 89)),  # first_response_at
                        datetime.now() - timedelta(days=np.random.randint(1, 88)),  # resolved_at
                        datetime.now() - timedelta(days=np.random.randint(1, 87)),  # closed_at
                        str(row.get('status', 'Closed')),
                        str(row.get('source', 'Email')),
                        str(row.get('type', 'General')),
                        str(row.get('subject', 'No subject'))[:200],
                        str(row.get('body', ''))[:1000],
                        240,  # expected_sla_resolve_min
                        30,   # sla_first_response_min
                        180,  # sla_resolution_min
                        np.random.randint(1, 10),  # agent_interactions
                        str(row.get('survey_results', 'Neutral')),
                        str(row.get('version', 'Standard')),
                        str(row.get('tag_1', 'Standard'))
                    )
                    
                    self.cursor.execute(sql, values)
                    inserted += 1
                    
                    if inserted % 100 == 0:
                        self.conn.commit()
                        print(f"  Inserted {inserted} tickets...")
                        
                except Exception as e:
                    print(f"Error inserting ticket: {e}")
                    continue
            
            self.conn.commit()
            print(f"Successfully loaded {inserted} tickets into fact_ticket")
            
        except Exception as e:
            print(f"Error loading tickets: {e}")
            self.conn.rollback()
    
    def load_fact_incidents(self, sample_size=None):
        """Load incident data into fact_incident table"""
        print("\n" + "="*60)
        print("LOADING INCIDENT DATA")
        print("="*60)
        
        try:
            source_file = os.path.join(self.base_path, "cleaned_incident_event_log.csv")
            df = pd.read_csv(source_file, low_memory=False)
            
            if sample_size:
                df = df.head(sample_size)
                print(f"Loading sample of {sample_size} incidents...")
            else:
                print(f"Loading {len(df)} incidents...")
            
            # Transform data
            df = self.transform_incident_data(df)
            
            # Get dimension mappings
            self.cursor.execute("SELECT priority_id, priority_name FROM dim_priority")
            priority_map = {row[1]: row[0] for row in self.cursor.fetchall()}
            
            # Insert incidents
            inserted = 0
            for _, row in df.iterrows():
                try:
                    priority_id = priority_map.get('Medium', 3)  # Default to Medium
                    
                    sql = """
                    INSERT INTO fact_incident 
                    (incident_id, ticket_id, client_id, site_id, priority_id,
                     opened_at, acknowledged_at, resolved_at, closed_at,
                     incident_state, active, reassignment_count, reopen_count, made_sla,
                     caller_id, contact_type, u_symptom, impact, urgency,
                     assignment_group, assigned_to, closed_code, resolved_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        int(row.get('incident_id', inserted + 1)),
                        None,  # ticket_id (can link later)
                        int(row.get('client_id', 1)),
                        int(row.get('site_id', 1)),
                        int(priority_id),
                        row.get('opened_at', datetime.now()),
                        row.get('acknowledged_at'),
                        row.get('resolved_at'),
                        row.get('closed_at'),
                        str(row.get('incident_state', 'new')),
                        int(row.get('active', 0)),
                        int(row.get('reassignment_count', 0)),
                        int(row.get('reopen_count', 0)),
                        int(row.get('made_sla', 1)),
                        str(row.get('caller_id', ''))[:50],
                        str(row.get('contact_type', 'email')),
                        str(row.get('u_symptom', ''))[:200],
                        int(row.get('impact', 3)),
                        int(row.get('urgency', 3)),
                        str(row.get('assignment_group', 'Service Desk'))[:100],
                        str(row.get('assigned_to', 'Unassigned'))[:100],
                        str(row.get('closed_code', 'Resolved'))[:50],
                        str(row.get('resolved_by', 'System'))[:100]
                    )
                    
                    self.cursor.execute(sql, values)
                    inserted += 1
                    
                    if inserted % 100 == 0:
                        self.conn.commit()
                        print(f"  Inserted {inserted} incidents...")
                        
                except Exception as e:
                    print(f"Error inserting incident: {e}")
                    continue
            
            self.conn.commit()
            print(f"Successfully loaded {inserted} incidents into fact_incident")
            
        except Exception as e:
            print(f"Error loading incidents: {e}")
            self.conn.rollback()
    
    def load_fact_metrics(self, sample_size=None):
        """Load metrics data into fact_metrics table"""
        print("\n" + "="*60)
        print("LOADING METRICS DATA")
        print("="*60)
        
        try:
            source_file = os.path.join(self.base_path, "cleaned_metrics_data.csv")
            df = pd.read_csv(source_file)
            
            if sample_size:
                df = df.head(sample_size)
                print(f"Loading sample of {sample_size} metrics...")
            else:
                print(f"Loading {len(df)} metrics...")
            
            # Clear existing data
            self.cursor.execute("DELETE FROM fact_metrics")
            
            # Insert metrics
            inserted = 0
            for _, row in df.iterrows():
                try:
                    sql = """
                    INSERT INTO fact_metrics 
                    (site_id, timestamp_utc, cpu_util_pct, mem_util_pct, 
                     link_util_pct, latency_ms, packet_loss_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        int(row.get('site_id', 1)),
                        pd.to_datetime(row.get('timestamp_utc', datetime.now())),
                        float(row.get('cpu_util_pct', 50.0)),
                        float(row.get('mem_util_pct', 60.0)),
                        float(row.get('link_util_pct', 40.0)),
                        float(row.get('latency_ms', 30.0)),
                        float(row.get('packet_loss_pct', 0.1))
                    )
                    
                    self.cursor.execute(sql, values)
                    inserted += 1
                    
                    if inserted % 1000 == 0:
                        self.conn.commit()
                        print(f"  Inserted {inserted} metrics...")
                        
                except Exception as e:
                    print(f"Error inserting metric: {e}")
                    continue
            
            self.conn.commit()
            print(f"Successfully loaded {inserted} metrics into fact_metrics")
            
        except Exception as e:
            print(f"Error loading metrics: {e}")
            self.conn.rollback()
    
    def run_etl(self, sample_size=100):
        """Run complete ETL pipeline"""
        print("="*60)
        print("STARTING ETL PIPELINE")
        print("="*60)
        
        if not self.connect_to_sql():
            return False
        
        try:
            # Step 1: Load dimension tables
            self.load_dimension_tables()
            
            # Step 2: Load fact tables with sample data first
            print("\nNOTE: Loading sample data for testing. Set sample_size=None for full load.")
            
            self.load_fact_tickets(sample_size=sample_size)
            self.load_fact_incidents(sample_size=sample_size)
            self.load_fact_metrics(sample_size=sample_size)
            
            # Step 3: Create audit log
            self.create_audit_log(sample_size)
            
            print("\n" + "="*60)
            print("ETL PIPELINE COMPLETED SUCCESSFULLY")
            print("="*60)
            
            return True
            
        except Exception as e:
            print(f"ETL Pipeline failed: {e}")
            return False
        finally:
            self.disconnect()
    
    def create_audit_log(self, sample_size):
        """Create audit log of ETL process"""
        try:
            audit_sql = """
            INSERT INTO etl_audit_log 
            (process_name, rows_processed, status, run_timestamp)
            VALUES (?, ?, ?, ?)
            """
            
            self.cursor.execute(audit_sql, (
                f"ITInfraETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                sample_size if sample_size else 'FULL',
                'COMPLETED',
                datetime.now()
            ))
            
            self.conn.commit()
            print("Audit log created")
            
        except:
            # Create audit table if it doesn't exist
            try:
                create_audit_sql = """
                CREATE TABLE etl_audit_log (
                    audit_id INT IDENTITY(1,1) PRIMARY KEY,
                    process_name NVARCHAR(200),
                    rows_processed NVARCHAR(50),
                    status NVARCHAR(50),
                    run_timestamp DATETIME,
                    notes NVARCHAR(500)
                )
                """
                self.cursor.execute(create_audit_sql)
                self.conn.commit()
                print("Created etl_audit_log table")
            except:
                print("Note: Could not create audit log table")

def main():
    """Main function to run ETL"""
    base_path = r"C:\Users\HP\OneDrive\Desktop\it-infra-sla-analytics"
    
    # Create ETL configuration first
    config_script = os.path.join(base_path, "src", "etl_config.py")
    if os.path.exists(config_script):
        print("Creating ETL configuration...")
        exec(open(config_script).read())
    
    # Run ETL pipeline
    etl = ITInfraETL(base_path)
    
    # Run with sample data first (100 rows each)
    print("\nRunning ETL with sample data (100 rows each)...")
    success = etl.run_etl(sample_size=100)
    
    if success:
        print("\nETL completed successfully with sample data.")
        print("\nTo run full ETL, change: etl.run_etl(sample_size=None)")
        print("Or run: etl.run_etl(sample_size=1000) for 1000 rows each")
    
    return success

if __name__ == "__main__":
    main()