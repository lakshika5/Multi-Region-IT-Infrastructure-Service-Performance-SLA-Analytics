# File: src/etl_config.py
"""
ETL Configuration for IT Infrastructure SLA Analytics
Maps CSV columns to SQL Server table columns
"""

ETL_CONFIG = {
    # ==================== DIMENSION TABLE MAPPINGS ====================
    "dimensions": {
        "dim_client": {
            "source_file": "data/generated/dim_clients.csv",
            "columns": ["client_id", "client_name", "industry", "size_segment", "hq_country", 
                       "region_group", "contract_tier", "monthly_contract_value", 
                       "sla_uptime_target", "response_time_sla_min", "resolution_time_sla_min",
                       "penalty_per_breach", "contract_start", "contract_end"]
        },
        "dim_site": {
            "source_file": "data/generated/dim_sites.csv",
            "columns": ["site_id", "client_id", "site_name", "country", "state_region", "city",
                       "site_type", "latitude", "longitude", "region_id"]
        },
        "dim_agent": {
            "source_file": "data/generated/dim_agents.csv",
            "columns": ["agent_id", "agent_name", "assignment_group", "team"]
        },
        "dim_priority": {
            "source_file": "data/generated/dim_priorities.csv",
            "columns": ["priority_id", "priority_name", "severity_rank"]
        },
        "dim_region": {
            "source_file": "data/generated/dim_regions.csv",
            "columns": ["region_id", "region_name", "country"]
        }
    },
    
    # ==================== FACT TABLE MAPPINGS ====================
    "facts": {
        "fact_ticket": {
            "source_files": ["cleaned_customer_tickets.csv"],
            "column_mapping": {
                # CSV Column -> SQL Column
                "subject": "subject",
                "body": "body",
                "survey_results": "survey_results",
                "type": "topic",
                "queue": "assignment_group",
                "priority": "priority_name",
                "source": "source",
                "version": "product_group"
            },
            "default_values": {
                "client_id": 1,  # Default to first client
                "site_id": 1,    # Default to first site
                "agent_id": 1,   # Default to first agent
                "priority_id": 3, # Default to Medium
                "status": "Closed",
                "support_level": "Standard"
            },
            "transformations": {
                "created_at": "CURRENT_TIMESTAMP",  # Use current time if no date
                "first_response_at": "DATEADD(MINUTE, 30, created_at)",  # Estimate
                "resolved_at": "DATEADD(HOUR, 2, created_at)",  # Estimate
                "closed_at": "DATEADD(MINUTE, 15, resolved_at)"  # Estimate
            }
        },
        
        "fact_incident": {
            "source_files": ["cleaned_incident_event_log.csv"],
            "column_mapping": {
                "incident_id": "incident_id",
                "incident_state": "incident_state",
                "active": "active",
                "reassignment_count": "reassignment_count",
                "reopen_count": "reopen_count",
                "modification_count": "modification_count",
                "made_sla": "made_sla",
                "caller_id": "caller_id",
                "opened_at": "opened_at",
                "acknowledged_at": "acknowledged_at",
                "resolved_at": "resolved_at",
                "closed_at": "closed_at",
                "contact_type": "contact_type",
                "u_symptom": "u_symptom",
                "impact": "impact",
                "urgency": "urgency",
                "assignment_group": "assignment_group",
                "assigned_to": "assigned_to",
                "closed_code": "closed_code",
                "resolved_by": "resolved_by"
            },
            "default_values": {
                "client_id": 1,
                "site_id": 1,
                "priority_id": 3,
                "ticket_id": None  # Will be linked if available
            }
        },
        
        "fact_metrics": {
            "source_files": ["cleaned_metrics_data.csv"],
            "column_mapping": {
                "metric_id": "metric_id",
                "site_id": "site_id",
                "timestamp_utc": "timestamp_utc",
                "cpu_util_pct": "cpu_util_pct",
                "mem_util_pct": "mem_util_pct",
                "link_util_pct": "link_util_pct",
                "latency_ms": "latency_ms",
                "packet_loss_pct": "packet_loss_pct"
            }
        }
    },
    
    # ==================== BUSINESS RULES ====================
    "business_rules": {
        "priority_mapping": {
            "1": "Critical",
            "2": "High",
            "3": "Medium",
            "4": "Low",
            "Critical": "Critical",
            "High": "High",
            "Medium": "Medium",
            "Low": "Low",
            "P1": "Critical",
            "P2": "High",
            "P3": "Medium",
            "P4": "Low"
        },
        "sla_thresholds": {
            "Critical": 60,    # minutes to resolve
            "High": 240,
            "Medium": 480,
            "Low": 1440
        },
        "region_assignments": {
            "India": ["India_North", "India_South", "India_West", "India_East"],
            "International": ["APAC", "Middle East", "Europe", "Americas"]
        }
    }
}

# Save as JSON for reference
import json
import os

config_path = r"C:\Users\HP\OneDrive\Desktop\it-infra-sla-analytics\etl_config.json"
with open(config_path, 'w') as f:
    json.dump(ETL_CONFIG, f, indent=2)

print(f"ETL configuration saved to: {config_path}")