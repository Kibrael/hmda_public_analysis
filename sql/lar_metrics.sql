SELECT count(*) AS application_count FROM lar_2004_ffiec; 
SELECT count(*) AS origination_count FROM lar_2004_ffiec WHERE action_type = '1';
SELECT SUM(CAST(loan_amount AS INTEGER)) AS application_value FROM lar_2004_ffiec;
SELECT SUM(CAST(loan_amount AS INTEGER)) AS origination_value FROM lar_2004_ffiec WHERE action_type = '1';
SELECT count(*) AS single_family_apps_count FROM lar_2004_ffiec WHERE property_type='1';
SELECT SUM(CAST(loan_amount AS INTEGER)) AS single_family_apps_value FROM lar_2004_ffiec WHERE property_type='1';
SELECT count(*) AS single_family_orig_count FROM lar_2004_ffiec WHERE property_type='1' AND action_type ='1';
SELECT SUM(CAST(loan_amount AS INTEGER)) AS single_family_orig_value FROM lar_2004_ffiec WHERE property_type='1' AND action_type ='1';
--do the above for each loan purpose
-- do the above for each property type (and/or product category)
-- combine loan purpose and property type
--#get top level metrics
--#count applications
--#total $ value applications
--#count originations
--#count $ value originations
--#count refi apps
--#count refi originations
--# $ value refi apps
--# $value refi originations
--#Race by Action taken
--# - count
--# - percentage of total
--# number of filers
