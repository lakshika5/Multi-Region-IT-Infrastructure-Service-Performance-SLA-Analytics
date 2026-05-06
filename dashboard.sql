-- First, create optimized views for Power BI (optional but recommended)
USE ITInfraSLAAnalytics;
GO

-- View for Operations Dashboard
CREATE OR ALTER VIEW vw_powerbi_operations AS
SELECT 
    t.ticket_id,
    t.client_id,
    c.client_name,
    c.industry,
    s.site_name,
    r.region_name,
    r.country,
    p.priority_name,
    t.created_at,
    t.resolved_at,
    t.status,
    DATEDIFF(HOUR, t.created_at, t.resolved_at) as resolution_hours,
    CASE WHEN DATEDIFF(HOUR, t.created_at, t.resolved_at) <= 24 THEN 'Within 24h' ELSE 'Over 24h' END as resolution_category,
    'Ticket' as record_type
FROM fact_ticket t
JOIN dim_client c ON t.client_id = c.client_id
JOIN dim_site s ON t.site_id = s.site_id
JOIN dim_region r ON s.region_id = r.region_id
JOIN dim_priority p ON t.priority_id = p.priority_id
WHERE t.resolved_at IS NOT NULL

UNION ALL

SELECT 
    i.incident_id,
    i.client_id,
    c.client_name,
    c.industry,
    s.site_name,
    r.region_name,
    r.country,
    p.priority_name,
    i.opened_at,
    i.resolved_at,
    i.incident_state as status,
    DATEDIFF(HOUR, i.opened_at, i.resolved_at) as resolution_hours,
    CASE WHEN DATEDIFF(HOUR, i.opened_at, i.resolved_at) <= 24 THEN 'Within 24h' ELSE 'Over 24h' END as resolution_category,
    'Incident' as record_type
FROM fact_incident i
JOIN dim_client c ON i.client_id = c.client_id
JOIN dim_site s ON i.site_id = s.site_id
JOIN dim_region r ON s.region_id = r.region_id
JOIN dim_priority p ON i.priority_id = p.priority_id
WHERE i.resolved_at IS NOT NULL;
GO

-- View for SLA Dashboard
CREATE OR ALTER VIEW vw_powerbi_sla AS
SELECT 
    c.client_name,
    c.industry,
    c.contract_tier,
    r.region_name,
    r.country,
    p.priority_name,
    YEAR(i.opened_at) as year,
    MONTH(i.opened_at) as month,
    COUNT(*) as incident_count,
    SUM(CASE WHEN i.made_sla = 1 THEN 1 ELSE 0 END) as sla_met,
    SUM(CASE WHEN i.made_sla = 0 THEN 1 ELSE 0 END) as sla_breached,
    AVG(DATEDIFF(HOUR, i.opened_at, i.resolved_at)) as avg_resolution_hours
FROM fact_incident i
JOIN dim_client c ON i.client_id = c.client_id
JOIN dim_site s ON i.site_id = s.site_id
JOIN dim_region r ON s.region_id = r.region_id
JOIN dim_priority p ON i.priority_id = p.priority_id
GROUP BY c.client_name, c.industry, c.contract_tier, r.region_name, r.country, 
         p.priority_name, YEAR(i.opened_at), MONTH(i.opened_at);
GO

-- View for Customer Health Dashboard
CREATE OR ALTER VIEW vw_powerbi_customer_health AS
SELECT 
    c.client_name,
    c.industry,
    c.contract_tier,
    c.monthly_contract_value,
    COALESCE(t.ticket_count, 0) as ticket_count,
    COALESCE(t.avg_resolution_hours, 0) as avg_ticket_resolution,
    COALESCE(t.satisfied_pct, 0) as satisfaction_rate,
    COALESCE(i.incident_count, 0) as incident_count,
    COALESCE(i.avg_resolution_hours, 0) as avg_incident_resolution,
    COALESCE(i.sla_compliance_pct, 0) as sla_compliance_pct
FROM dim_client c
LEFT JOIN (
    SELECT 
        client_id,
        COUNT(*) as ticket_count,
        AVG(DATEDIFF(HOUR, created_at, resolved_at)) as avg_resolution_hours,
        CAST(SUM(CASE WHEN survey_results = 'Satisfied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as satisfied_pct
    FROM fact_ticket
    GROUP BY client_id
) t ON c.client_id = t.client_id
LEFT JOIN (
    SELECT 
        client_id,
        COUNT(*) as incident_count,
        AVG(DATEDIFF(HOUR, opened_at, resolved_at)) as avg_resolution_hours,
        CAST(SUM(CASE WHEN made_sla = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as sla_compliance_pct
    FROM fact_incident
    GROUP BY client_id
) i ON c.client_id = i.client_id;
GO