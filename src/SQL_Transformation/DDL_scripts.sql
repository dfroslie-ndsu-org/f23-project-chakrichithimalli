IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = 'f23_project')
    EXEC('CREATE SCHEMA f23_project;');

IF OBJECT_ID('f23_project.US_Treasury_details', 'U') IS NOT NULL
    DROP TABLE f23_project.US_Treasury_details;

IF EXISTS (SELECT 1 FROM sys.sequences WHERE name = 'US_Treasury_details_sequence' AND schema_id = SCHEMA_ID('f23_project'))
    DROP SEQUENCE f23_project.US_Treasury_details_sequence ;

--Creating sequence for US_Treasury_details
CREATE SEQUENCE f23_project.US_Treasury_details_sequence
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 1000000
    START WITH 1;

-- Creating table US_Treasury_details
CREATE TABLE f23_project.US_Treasury_details (
                                                 id BIGINT PRIMARY KEY DEFAULT NEXT VALUE FOR f23_project.US_Treasury_details_sequence,
                                                 record_date DATE,
                                                 security_type_desc varchar(30),
                                                 security_desc varchar(100),
                                                 avg_interest_rate_amt FLOAT,  -- Adjusted precision and scale
                                                 record_fiscal_year_df2 int,
                                                 record_fiscal_quarter_df2 int,
                                                 record_calendar_year_df2 int,
                                                 record_calendar_quarter_df2 int,
                                                 record_calendar_month_df2 int,
                                                 record_calendar_day_df2 int,
                                                 debt_held_public_mil_amt FLOAT,
                                                 intragov_hold_mil_amt FLOAT,
                                                 total_mil_amt FLOAT
);