USE ITInfraSLAAnalytics;

-- Clients
CREATE TABLE dim_client (
    client_id INT IDENTITY(1,1) PRIMARY KEY,
    client_name NVARCHAR(255),
    industry NVARCHAR(100),
    size_segment NVARCHAR(50),
    hq_country NVARCHAR(100),
    region_group NVARCHAR(50),
    contract_tier NVARCHAR(50)
);
ALTER TABLE dim_client ADD monthly_revenue DECIMAL(10,2) NULL;

-- Regions
CREATE TABLE dim_region (
    region_id INT IDENTITY(1,1) PRIMARY KEY,
    region_name NVARCHAR(100),
    country NVARCHAR(100)
);

-- Sites
CREATE TABLE dim_site (
    site_id INT IDENTITY(1,1) PRIMARY KEY,
    client_id INT REFERENCES dim_client(client_id),
    site_name NVARCHAR(255),
    country NVARCHAR(100),
    state_region NVARCHAR(100),
    city NVARCHAR(100),
    region_id INT REFERENCES dim_region(region_id),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    site_type NVARCHAR(50)
);

-- Agents
CREATE TABLE dim_agent (
    agent_id INT IDENTITY(1,1) PRIMARY KEY,
    agent_name NVARCHAR(255),
    assignment_group NVARCHAR(100),
    team NVARCHAR(100)
);

-- Priority
CREATE TABLE dim_priority (
    priority_id INT IDENTITY(1,1) PRIMARY KEY,
    priority_name NVARCHAR(50),
    severity_rank INT
);

-- Time (date grain)
CREATE TABLE dim_time (
    time_id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);