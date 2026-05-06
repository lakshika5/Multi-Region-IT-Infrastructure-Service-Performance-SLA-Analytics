USE ITInfraSLAAnalytics;

-- Create validation log table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='data_validation_log' AND xtype='U')
BEGIN
    CREATE TABLE data_validation_log (
        validation_id INT IDENTITY(1,1) PRIMARY KEY,
        validation_date DATETIME DEFAULT GETDATE(),
        table_name NVARCHAR(100),
        validation_type NVARCHAR(100),
        validation_metric NVARCHAR(200),
        metric_value DECIMAL(20,4),
        threshold_value DECIMAL(20,4),
        validation_status NVARCHAR(20),
        notes NVARCHAR(500)
    );
    
    PRINT 'Created data_validation_log table';
END
GO

-- Clear previous validations
DELETE FROM data_validation_log;
PRINT 'Cleared previous validation records';
GO

-- Function to log validation results
CREATE OR ALTER PROCEDURE log_validation_result
    @table_name NVARCHAR(100),
    @validation_type NVARCHAR(100),
    @validation_metric NVARCHAR(200),
    @metric_value DECIMAL(20,4),
    @threshold_value DECIMAL(20,4),
    @notes NVARCHAR(500) = NULL
AS
BEGIN
    DECLARE @status NVARCHAR(20);
    
    -- Determine status based on threshold
    IF @threshold_value = 0 
        SET @status = CASE WHEN @metric_value = 0 THEN 'PASS' ELSE 'FAIL' END;
    ELSE
        SET @status = CASE WHEN @metric_value <= @threshold_value THEN 'PASS' ELSE 'FAIL' END;
    
    INSERT INTO data_validation_log 
    (table_name, validation_type, validation_metric, metric_value, threshold_value, validation_status, notes)
    VALUES (@table_name, @validation_type, @validation_metric, @metric_value, @threshold_value, @status, @notes);
END
GO