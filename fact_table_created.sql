USE ITInfraSLAAnalytics;


-- Tickets
CREATE TABLE fact_ticket (
    ticket_id BIGINT PRIMARY KEY, -- keep original ticket id if available
    client_id INT REFERENCES dim_client(client_id),
    site_id INT REFERENCES dim_site(site_id),
    agent_id INT REFERENCES dim_agent(agent_id),
    priority_id INT REFERENCES dim_priority(priority_id),
    created_at DATETIME2(3),
    first_response_at DATETIME2(3),
    resolved_at DATETIME2(3),
    closed_at DATETIME2(3),
    status NVARCHAR(50),
    source NVARCHAR(50),
    topic NVARCHAR(200),
    subject NVARCHAR(400),
    body NVARCHAR(MAX),
    expected_sla_resolve_min INT,
    sla_first_response_min INT,
    sla_resolution_min INT,
    agent_interactions INT,
    survey_results NVARCHAR(50),
    product_group NVARCHAR(100),
    support_level NVARCHAR(50)
);

-- Incidents
CREATE TABLE fact_incident (
    incident_id BIGINT PRIMARY KEY,
    ticket_id BIGINT NULL REFERENCES fact_ticket(ticket_id),
    client_id INT REFERENCES dim_client(client_id),
    site_id INT REFERENCES dim_site(site_id),
    priority_id INT REFERENCES dim_priority(priority_id),
    opened_at DATETIME2(3),
    acknowledged_at DATETIME2(3),
    resolved_at DATETIME2(3),
    closed_at DATETIME2(3),
    incident_state NVARCHAR(50),
    active BIT,
    reassignment_count INT,
    reopen_count INT,
    made_sla BIT,
    caller_id NVARCHAR(100),
    contact_type NVARCHAR(50),
    location NVARCHAR(200),
    category NVARCHAR(100),
    sub_category NVARCHAR(100),
    u_symptom NVARCHAR(200),
    impact INT,
    urgency INT,
    assignment_group NVARCHAR(100),
    assigned_to NVARCHAR(100),
    closed_code NVARCHAR(50),
    resolved_by NVARCHAR(100)
);

-- Optional: Infra metrics (synthetic telemetry)
CREATE TABLE fact_metrics (
    metric_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    site_id INT REFERENCES dim_site(site_id),
    timestamp_utc DATETIME2(3),
    cpu_util_pct DECIMAL(5,2),
    mem_util_pct DECIMAL(5,2),
    link_util_pct DECIMAL(5,2),
    latency_ms DECIMAL(8,2),
    packet_loss_pct DECIMAL(5,2)
);