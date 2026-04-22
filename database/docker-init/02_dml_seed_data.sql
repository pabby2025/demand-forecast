-- =============================================================================
-- DEMAND FORECAST PLANNER — Seed / Reference Data (DML)
-- Run after 01_ddl_schema.sql
-- =============================================================================

SET search_path = dfc, public;

-- ---------------------------------------------------------------------------
-- dim_practice_area
-- Source: DEFAULT_FILTERS["Practice Area"] in preprocess.py
-- ---------------------------------------------------------------------------
INSERT INTO dfc.dim_practice_area (pa_code, pa_name) VALUES
  ('ADM', 'Application Development & Modernization'),
  ('DE',  'Digital Engineering'),
  ('EPS', 'Enterprise Platform Services')
ON CONFLICT (pa_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_market
-- Source: Market column after GGM conversion + APJ drop (preprocess.py step 10)
-- ---------------------------------------------------------------------------
INSERT INTO dfc.dim_market (market_code, region_desc) VALUES
  ('Americas', 'North & South America (US, Canada, Brazil, etc.)'),
  ('EMEA',     'Europe, Middle East & Africa (UK, Germany, Poland, UAE, etc.)')
ON CONFLICT (market_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_so_grade
-- Source: SO GRADE column normalized in preprocess.py
-- DEFAULT_GRADES_TO_COMBINE: PT/PAT/PA/P → GenC
-- DEFAULT_GRADES_TO_REMOVE:  cont, D, SR. DIR., VP, AVP, Admin Staff, TD Trainee
-- ---------------------------------------------------------------------------
INSERT INTO dfc.dim_so_grade (grade_code, grade_label, grade_raw_values, sort_order) VALUES
  ('A',    'Analyst',            ARRAY['A'],                      1),
  ('GenC', 'General Consultant', ARRAY['GenC','PT','PAT','PA','P'], 2),
  ('SA',   'Senior Associate',   ARRAY['SA'],                     3),
  ('M',    'Manager',            ARRAY['M'],                      4),
  ('SM',   'Senior Manager',     ARRAY['SM'],                     5),
  ('AD',   'Associate Director', ARRAY['AD'],                     6)
ON CONFLICT (grade_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_business_unit — Americas BUs
-- Source: SBU-BU Mapping.xlsx + preprocess.py step 9 (Americas BU correction)
--         Actual BU list from backend/mock_data.py BUSINESS_UNITS + contracts
-- ---------------------------------------------------------------------------
WITH am AS (SELECT market_id FROM dfc.dim_market WHERE market_code = 'Americas')
INSERT INTO dfc.dim_business_unit (bu_name, market_id) VALUES
  ('Banking & Capital Markets - NA',  (SELECT market_id FROM am)),
  ('Financial Services & FinTech - NA', (SELECT market_id FROM am)),
  ('Insurance - NA',                  (SELECT market_id FROM am)),
  ('ML - NA',                         (SELECT market_id FROM am)),
  ('Provider BU',                     (SELECT market_id FROM am)),
  ('Retail NA',                       (SELECT market_id FROM am)),
  ('Technology NA',                   (SELECT market_id FROM am)),
  ('Consulting',                      (SELECT market_id FROM am)),
  ('Healthcare',                      (SELECT market_id FROM am)),
  ('Manufacturing',                   (SELECT market_id FROM am)),
  ('Energy',                          (SELECT market_id FROM am)),
  ('Telecom',                         (SELECT market_id FROM am)),
  ('Government',                      (SELECT market_id FROM am)),
  ('Logistics',                       (SELECT market_id FROM am))
ON CONFLICT (bu_name, market_id) DO NOTHING;

-- EMEA BUs (after preprocess.py corrections:
--   Mobility dropped, Transport-UK → T&H-UK, RCGT&H-UK merge, South Europe → SPAI, Benelux split)
WITH em AS (SELECT market_id FROM dfc.dim_market WHERE market_code = 'EMEA')
INSERT INTO dfc.dim_business_unit (bu_name, market_id) VALUES
  ('T&H-UK',      (SELECT market_id FROM em)),
  ('RCGT&H-UK',   (SELECT market_id FROM em)),
  ('SPAI',        (SELECT market_id FROM em)),   -- South Europe + Spain
  ('Belux',       (SELECT market_id FROM em)),   -- Belgium + Luxembourg (Benelux split)
  ('Netherlands', (SELECT market_id FROM em)),   -- Netherlands (Benelux split)
  ('DACH',        (SELECT market_id FROM em)),
  ('Nordics',     (SELECT market_id FROM em)),
  ('France',      (SELECT market_id FROM em)),
  ('UAE-EMEA',    (SELECT market_id FROM em))
ON CONFLICT (bu_name, market_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_country
-- Source: Country column; Off/ On → delivery_mode
--         Top countries from contracts/ml-api-contract.md
-- ---------------------------------------------------------------------------
WITH am AS (SELECT market_id FROM dfc.dim_market WHERE market_code = 'Americas'),
     em AS (SELECT market_id FROM dfc.dim_market WHERE market_code = 'EMEA')
INSERT INTO dfc.dim_country (country_name, iso2_code, market_id, default_delivery_mode) VALUES
  ('US',          'US', (SELECT market_id FROM am), 'Onsite'),
  ('Canada',      'CA', (SELECT market_id FROM am), 'Onsite'),
  ('India',       'IN', (SELECT market_id FROM am), 'Offshore'),  -- Offshore for Americas
  ('Philippines', 'PH', (SELECT market_id FROM am), 'Offshore'),
  ('UK',          'GB', (SELECT market_id FROM em), 'Onsite'),
  ('Poland',      'PL', (SELECT market_id FROM em), 'Offshore'),
  ('Germany',     'DE', (SELECT market_id FROM em), 'Onsite'),
  ('Australia',   'AU', NULL, 'Onsite'),
  ('Singapore',   'SG', NULL, 'Offshore'),
  ('UAE',         'AE', (SELECT market_id FROM em), 'Onsite')
ON CONFLICT (country_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_skill_cluster  — 9 canonical Skill Micro Clusters
-- Source: ml-services/reference-data/skill_clusters.json
--         Built by skill_clusters_demand.py + apply_clusters.py
-- Note: CV scores and mapped_demand values from actual pipeline outputs;
--       these are updated by the pipeline after each run.
-- PA = ADM assumed for initial seed (clusters span all PAs; update pa_id as needed)
-- ---------------------------------------------------------------------------
WITH
  de_pa  AS (SELECT pa_id FROM dfc.dim_practice_area WHERE pa_code = 'DE'),
  adm_pa AS (SELECT pa_id FROM dfc.dim_practice_area WHERE pa_code = 'ADM'),
  am_mkt AS (SELECT market_id FROM dfc.dim_market WHERE market_code = 'Americas')
INSERT INTO dfc.dim_skill_cluster
  (cluster_name, pa_id, market_id, num_skills, mapped_demand, cv_score, cv_2025, xyz_segment)
VALUES
  -- Data from ml-services/reference-data/skill_clusters.json + contracts/ml-api-contract.md
  ('MSC-.NET-Angular-Azure-C#-Java',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    5, 7421, 0.2510, 0.2170, 'X'),
  ('MSC-Agile-Microsoft_365-PPM-Project_Management',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    4, 6363, 0.3080, 0.3330, 'X'),
  ('MSC-Git-HTML/CSS-Node_JS-React-TypeScript',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    5, 4884, 0.3460, 0.3030, 'X'),
  ('MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    5, 1339, 0.3950, 0.1940, 'X'),
  ('MSC-Java-Kafka-Microservices-Python-Spring_Boot',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    5, 5250, 0.4710, 0.3560, 'X'),
  ('MSC-AWS-Java-MySQL-SQL-Spring_Boot',
    (SELECT pa_id FROM adm_pa), (SELECT market_id FROM am_mkt),
    5, 15618, 0.5310, 0.1740, 'Y'),
  ('MSC-Android-React_Native-iOS',
    (SELECT pa_id FROM de_pa), (SELECT market_id FROM am_mkt),
    3, 2119, 0.5330, 0.3820, 'Y'),
  ('MSC-API_Development-Git-Java-Shell_Scripting/Linux-Software_Testing',
    (SELECT pa_id FROM adm_pa), (SELECT market_id FROM am_mkt),
    5, 1864, 0.7220, 0.3500, 'Y'),
  ('MSC-AWS-Java-JavaScript-MySQL-SQL',
    (SELECT pa_id FROM adm_pa), (SELECT market_id FROM am_mkt),
    5, 2562, 0.8260, 0.2400, 'Y')
ON CONFLICT (cluster_name, pa_id, market_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_skill — Core normalized skills (top skills from Americas_ADM)
-- Source: skills/Americas_ADM/total_skills.json, high_demand_skills.json
--         and skill_normalization_llm2.json
-- ---------------------------------------------------------------------------
INSERT INTO dfc.dim_skill
  (skill_name, skill_name_variants, is_high_demand, demand_2023, demand_2024, demand_2025)
VALUES
  ('Java',              ARRAY['java','J2EE','Java EE','Core Java'],            TRUE, 5200, 6800, 6979),
  ('Spring Boot',       ARRAY['SpringBoot','Spring-Boot','Spring Boot MVC'],   TRUE, 4100, 5200, 5304),
  ('Microservices',     ARRAY['Micro Services','Microservice','SOA/Microservices'], TRUE, 2800, 3600, 4182),
  ('.NET',              ARRAY['.net','.NET Framework','ASP.NET','.NET Core'],  TRUE, 2900, 3500, 3836),
  ('Oracle',            ARRAY['Oracle DB','Oracle Database','Oracle RDBMS'],   TRUE, 2400, 2900, 3044),
  ('SQL',               ARRAY['SQL Server','SQL/PLSQL','PL/SQL','T-SQL'],      TRUE, 1900, 2400, 2936),
  ('COBOL',             ARRAY['COBOL/JCL','Cobol','IBM COBOL'],                TRUE, 1200, 1500, 1749),
  ('Angular',           ARRAY['AngularJS','Angular 2+','Angular JS'],          TRUE, 850, 1100, 1207),
  ('AWS',               ARRAY['Amazon Web Services','Amazon AWS','AWS Cloud'], TRUE, 1100, 1400, 1873),
  ('Azure',             ARRAY['Microsoft Azure','Azure Cloud'],                 TRUE, 700, 950, 1189),
  ('Python',            ARRAY['python3','Python 3','Python/Django'],            TRUE, 900, 1200, 1583),
  ('React',             ARRAY['ReactJS','React.js','React JS'],                 TRUE, 600, 850, 1135),
  ('Node JS',           ARRAY['NodeJS','Node.js','Node'],                       TRUE, 550, 750, 1028),
  ('JavaScript',        ARRAY['JS','ES6','ECMAScript'],                        TRUE, 650, 850, 1049),
  ('TypeScript',        ARRAY['TS','Typescript'],                               TRUE, 400, 600, 788),
  ('Docker',            ARRAY['Docker Containers','Docker/Kubernetes'],         TRUE, 350, 550, 712),
  ('Kubernetes',        ARRAY['K8s','k8s','kube'],                              TRUE, 280, 430, 589),
  ('Terraform',         ARRAY['Terraform IaC','HashiCorp Terraform'],          TRUE, 200, 340, 456),
  ('Jenkins',           ARRAY['Jenkins CI/CD','Jenkins Pipeline'],             TRUE, 270, 370, 489),
  ('Kafka',             ARRAY['Apache Kafka','Kafka Streaming'],                TRUE, 320, 450, 678),
  ('MySQL',             ARRAY['MySQL DB','MySQL Database'],                     TRUE, 480, 620, 789),
  ('ServiceNow',        ARRAY['SNOW','Service Now','ServiceNow ITSM'],          TRUE, 580, 720, 985),
  ('DevOps',            ARRAY['Dev Ops','DevOps Engineering'],                  TRUE, 420, 560, 712),
  ('Agile',             ARRAY['Agile Methodology','Scrum/Agile'],               TRUE, 500, 650, 834),
  ('C#',                ARRAY['C Sharp','C#.NET'],                              TRUE, 780, 950, 1134),
  ('HTML/CSS',          ARRAY['HTML5/CSS3','HTML & CSS','HTML CSS'],            TRUE, 390, 510, 634),
  ('Git',               ARRAY['GitHub','GitLab','Git SCM'],                     TRUE, 450, 580, 723),
  ('SAP',               ARRAY['SAP ERP','SAP ABAP','SAP S/4HANA'],             TRUE, 680, 820, 967),
  ('Pega',              ARRAY['Pega BPM','Pega Platform','Pega PRPC'],         TRUE, 340, 430, 521),
  ('ABAP',              ARRAY['SAP ABAP','ABAP/4','ABAP OO'],                  TRUE, 290, 370, 445),
  ('DB2',               ARRAY['IBM DB2','DB2 LUW','DB2 z/OS'],                 TRUE, 890, 1050, 1186),
  ('Shell Scripting/Linux', ARRAY['Shell Script','Bash Scripting','Linux/Shell'], TRUE, 580, 730, 905),
  ('VSAM',              ARRAY['IBM VSAM','VSAM File Processing'],               TRUE, 710, 870, 1071),
  ('JCL',               ARRAY['IBM JCL','Job Control Language'],                TRUE, 820, 980, 1168),
  ('Android',           ARRAY['Android SDK','Android Development'],             TRUE, 530, 650, 789),
  ('iOS',               ARRAY['iOS Development','Swift/iOS'],                   TRUE, 420, 520, 634),
  ('React Native',      ARRAY['ReactNative','React-Native'],                    TRUE, 340, 440, 534),
  ('Project Management',ARRAY['Project Mgmt','PM','PMP'],                       TRUE, 620, 780, 934),
  ('Agile',             NULL, TRUE, 0, 0, 0),  -- duplicate guard; ON CONFLICT handles
  ('PPM',               ARRAY['Portfolio & Project Management','PPM Tools'],    TRUE, 410, 510, 623),
  ('Microsoft 365',     ARRAY['M365','MS365','Microsoft Office 365'],           TRUE, 380, 470, 567),
  ('Software Testing',  ARRAY['QA Testing','Software QA','Test Engineering'],  TRUE, 450, 580, 712),
  ('API Development',   ARRAY['API Dev','REST API Development','API Engineering'], TRUE, 560, 700, 867)
ON CONFLICT (skill_name) DO UPDATE SET
  skill_name_variants = EXCLUDED.skill_name_variants,
  is_high_demand      = EXCLUDED.is_high_demand,
  demand_2023         = EXCLUDED.demand_2023,
  demand_2024         = EXCLUDED.demand_2024,
  demand_2025         = EXCLUDED.demand_2025,
  updated_at          = now();

-- ---------------------------------------------------------------------------
-- dim_cluster_skill_map — Leaf skills per cluster
-- Source: ml-services/reference-data/skill_clusters.json clusters[] arrays
--         + backend/mock_data.py CLUSTER_LEAF_SKILLS (weights from Jaccard)
-- ---------------------------------------------------------------------------
-- Cluster: MSC-.NET-Angular-Azure-C#-Java
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT
  sc.cluster_id,
  s.skill_id,
  v.weight,
  v.is_priority,
  v.is_primary
FROM (VALUES
  ('.NET',    0.91, FALSE, TRUE),
  ('Angular', 0.87, FALSE, TRUE),
  ('Azure',   0.83, FALSE, TRUE),
  ('C#',      0.95, FALSE, TRUE),
  ('Java',    0.78, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-.NET-Angular-Azure-C#-Java'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-Agile-Microsoft_365-PPM-Project_Management
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('Agile',            0.94, FALSE, TRUE),
  ('Microsoft 365',    0.88, FALSE, TRUE),
  ('PPM',              0.79, FALSE, TRUE),
  ('Project Management', 0.96, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-Agile-Microsoft_365-PPM-Project_Management'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-Git-HTML/CSS-Node_JS-React-TypeScript
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('Git',        0.93, FALSE, TRUE),
  ('HTML/CSS',   0.89, FALSE, TRUE),
  ('Node JS',    0.86, FALSE, TRUE),
  ('React',      0.92, FALSE, TRUE),
  ('TypeScript', 0.88, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-Git-HTML/CSS-Node_JS-React-TypeScript'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('AWS',       0.90, FALSE, TRUE),
  ('DevOps',    0.88, FALSE, TRUE),
  ('Docker',    0.85, FALSE, TRUE),
  ('Jenkins',   0.79, FALSE, TRUE),
  ('Terraform', 0.82, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-AWS-DevOps-Docker_Containers-Jenkins-Terraform'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-Java-Kafka-Microservices-Python-Spring_Boot
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('Java',         0.95, FALSE, TRUE),
  ('Kafka',        0.81, FALSE, TRUE),
  ('Microservices',0.87, FALSE, TRUE),
  ('Python',       0.83, FALSE, TRUE),
  ('Spring Boot',  0.90, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-Java-Kafka-Microservices-Python-Spring_Boot'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-AWS-Java-MySQL-SQL-Spring_Boot
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('AWS',        0.88, FALSE, TRUE),
  ('Java',       0.94, FALSE, TRUE),
  ('MySQL',      0.86, FALSE, TRUE),
  ('SQL',        0.92, FALSE, TRUE),
  ('Spring Boot',0.89, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-AWS-Java-MySQL-SQL-Spring_Boot'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-Android-React_Native-iOS
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('Android',      0.91, TRUE, TRUE),   -- Priority skill per PA_PRIORITY_SKILLS DE
  ('React Native', 0.87, FALSE, TRUE),
  ('iOS',          0.89, TRUE, TRUE)    -- Priority skill
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-Android-React_Native-iOS'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-API_Development-Git-Java-Shell_Scripting/Linux-Software_Testing
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('API Development',      0.90, FALSE, TRUE),
  ('Git',                  0.88, FALSE, TRUE),
  ('Java',                 0.85, FALSE, TRUE),
  ('Shell Scripting/Linux',0.79, FALSE, TRUE),
  ('Software Testing',     0.87, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-API_Development-Git-Java-Shell_Scripting/Linux-Software_Testing'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- Cluster: MSC-AWS-Java-JavaScript-MySQL-SQL
INSERT INTO dfc.dim_cluster_skill_map (cluster_id, skill_id, weight, is_priority, is_primary)
SELECT sc.cluster_id, s.skill_id, v.weight, v.is_priority, v.is_primary
FROM (VALUES
  ('AWS',        0.87, FALSE, TRUE),
  ('Java',       0.91, FALSE, TRUE),
  ('JavaScript', 0.85, FALSE, TRUE),
  ('MySQL',      0.88, FALSE, TRUE),
  ('SQL',        0.93, FALSE, TRUE)
) AS v(skill_name, weight, is_priority, is_primary)
JOIN dfc.dim_skill s ON s.skill_name = v.skill_name
JOIN dfc.dim_skill_cluster sc ON sc.cluster_name = 'MSC-AWS-Java-JavaScript-MySQL-SQL'
ON CONFLICT (cluster_id, skill_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- app_user — Seed demo users
-- ---------------------------------------------------------------------------
INSERT INTO dfc.app_user (email, name, role) VALUES
  ('sl.coo@cognizant.com',     'Service Line COO',    'SL_COO'),
  ('market.coo@cognizant.com', 'Market COO',          'MARKET_COO'),
  ('cft.planner@cognizant.com','CFT Planner',         'CFT_PLANNER')
ON CONFLICT (email) DO NOTHING;
