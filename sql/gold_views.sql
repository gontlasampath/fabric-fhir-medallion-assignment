-- Run in the Lakehouse SQL analytics endpoint after Spark tables synchronize.
CREATE OR ALTER VIEW dbo.v_patient AS SELECT * FROM dbo.gold_patient;
GO
CREATE OR ALTER VIEW dbo.v_encounter AS SELECT * FROM dbo.gold_encounter;
GO
CREATE OR ALTER VIEW dbo.v_observation AS SELECT * FROM dbo.gold_observation;
GO
CREATE OR ALTER VIEW dbo.v_condition AS SELECT * FROM dbo.gold_condition;
GO
CREATE OR ALTER VIEW dbo.v_encounter_analytics AS SELECT * FROM dbo.gold_encounter_analytics;
GO
