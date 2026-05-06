USE ITInfraSLAAnalytics;
GO

-- View: MTTA and MTTR per ticket
CREATE VIEW vw_ticket_kpis AS
SELECT
    t.ticket_id,
    c.client_name,
    s.site_name,
    a.agent_name,
    p.priority_name,
    DATEDIFF(MINUTE, t.created_at, t.first_response_at) AS mtta_minutes,
    DATEDIFF(MINUTE, t.created_at, t.resolved_at) AS mttr_minutes,
    CASE WHEN DATEDIFF(MINUTE, t.created_at, t.first_response_at) > t.sla_first_response_min THEN 1 ELSE 0 END AS response_breach,
    CASE WHEN DATEDIFF(MINUTE, t.created_at, t.resolved_at) > t.sla_resolution_min THEN 1 ELSE 0 END AS resolution_breach
FROM fact_ticket t
JOIN dim_client c ON t.client_id = c.client_id
JOIN dim_site s ON t.site_id = s.site_id
JOIN dim_agent a ON t.agent_id = a.agent_id
JOIN dim_priority p ON t.priority_id = p.priority_id;
GO

-- View: MTTR per incident
CREATE VIEW vw_incident_kpis AS
SELECT
    i.incident_id,
    c.client_name,
    s.site_name,
    p.priority_name,
    DATEDIFF(MINUTE, i.opened_at, i.acknowledged_at) AS mtta_minutes,
    DATEDIFF(MINUTE, i.opened_at, i.resolved_at) AS mttr_minutes,
    i.reassignment_count,
    i.reopen_count,
    i.made_sla
FROM fact_incident i
JOIN dim_client c ON i.client_id = c.client_id
JOIN dim_site s ON i.site_id = s.site_id
JOIN dim_priority p ON i.priority_id = p.priority_id;
GO

SELECT * FROM vw_ticket_kpis;
SELECT * FROM vw_incident_kpis;