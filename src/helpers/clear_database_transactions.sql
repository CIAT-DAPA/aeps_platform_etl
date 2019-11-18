SET FOREIGN_KEY_CHECKS = 0; 

truncate table far_responses_text;
truncate table far_responses_options;
truncate table far_responses_numeric;
truncate table far_responses_date;
truncate table far_responses_bool;
truncate table far_production_events;
truncate table far_plots;
truncate table far_farms;
truncate table soc_technical_assistants;
truncate table soc_people;
truncate table soc_associations;
truncate table con_municipalities;
truncate table con_states;
truncate table con_countries;

ALTER TABLE far_responses_text AUTO_INCREMENT=0;
ALTER TABLE far_responses_options AUTO_INCREMENT=0;
ALTER TABLE far_responses_numeric AUTO_INCREMENT=0;
ALTER TABLE far_responses_date AUTO_INCREMENT=0;
ALTER TABLE far_responses_bool AUTO_INCREMENT=0;
ALTER TABLE far_production_events AUTO_INCREMENT=0;
ALTER TABLE far_plots AUTO_INCREMENT=0;
ALTER TABLE far_farms AUTO_INCREMENT=0;
ALTER TABLE soc_technical_assistants AUTO_INCREMENT=0;
ALTER TABLE soc_people AUTO_INCREMENT=0;
ALTER TABLE soc_associations AUTO_INCREMENT=0;
ALTER TABLE con_municipalities AUTO_INCREMENT=0;
ALTER TABLE con_states AUTO_INCREMENT=0;
ALTER TABLE con_countries AUTO_INCREMENT=0;

SET FOREIGN_KEY_CHECKS = 1;