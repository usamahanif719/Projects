-- Database: dap_project_database

-- DROP DATABASE IF EXISTS dap_project_database;

	
DROP FUNCTION IF EXISTS public.extract_month_name(date);
CREATE OR REPLACE FUNCTION public.extract_month_name(adate date)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
SELECT to_char(adate,'Month');
$BODY$;

DROP FUNCTION IF EXISTS public.extract_day_name(date);
CREATE OR REPLACE FUNCTION public.extract_day_name(adate date)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
SELECT to_char(adate,'Day');
$BODY$;

DROP TABLE IF EXISTS public.drivers_dimension;
CREATE TABLE public.drivers_dimension
(   
	IDNumber SERIAL PRIMARY KEY,
	report_number character varying(100) NOT NULL,
    local_case_number character varying(100) NOT NULL,
    agency_name character varying(100) NOT NULL,
    acrs_report_type character varying(100) NOT NULL,
    crash_date_time character varying(100) NOT NULL,
    route_type character varying(100) NOT NULL,
--     road_name character varying(100) NOT NULL,
--     cross_street_type character varying(100) NOT NULL,
--     cross_street_name character varying(100) NOT NULL,
--     off_road_description character varying(100) NOT NULL,
--     municipality character varying(100) NOT NULL,
--     related_non_motorist character varying(100) NOT NULL,
    collision_type character varying(100) NOT NULL,
    weather character varying(100) NOT NULL,
    surface_condition character varying(100) NOT NULL,
    light character varying(100) NOT NULL,
    traffic_control character varying(100) NOT NULL,
    driver_substance_abuse character varying(100) NOT NULL,
    non_motorist_substance_abuse character varying(100) NOT NULL,
    person_id character varying(100) NOT NULL,
    driver_at_fault character varying(100) NOT NULL,
    injury_severity character varying(100) NOT NULL,
--     circumstance character varying(100) NOT NULL,
    driver_distracted_by character varying(100) NOT NULL,
    drivers_license_state character varying(100) NOT NULL,
    vehicle_id character varying(100) NOT NULL,
    vehicle_damage_extent character varying(100) NOT NULL,
--     vehicle_first_impact_location character varying(100) NOT NULL,
--     vehicle_second_impact_location character varying(100) NOT NULL,
    vehicle_body_type character varying(100) NOT NULL,
    vehicle_movement character varying(100) NOT NULL,
--     vehicle_continuing_dir character varying(100) NOT NULL,
--     vehicle_going_dir character varying(100) NOT NULL,
    speed_limit character varying(100) NOT NULL,
    driverless_vehicle character varying(100) NOT NULL,
--     parked_vehicle character varying(100) NOT NULL,
    vehicle_year character varying(100) NOT NULL, 
    vehicle_make character varying(100) NOT NULL, 
    vehicle_model character varying(100) NOT NULL,
    equipment_problems character varying(100) NOT NULL);
--     latitude character varying(100) NOT NULL,
--     longitude character varying(100) NOT NULL,
--     location character varying(100) NOT NULL, 
-- 	 PRIMARY KEY(IDNumber)
--     CONSTRAINT pk_drivers PRIMARY KEY (report_number)



DROP TABLE IF EXISTS public.incidents_dimension;
CREATE TABLE public.incidents_dimension
(   
-- 	IDNumber SERIAL PRIMARY KEY,
-- 	report_number char PRIMARY KEY,
	report_number character varying(100) NOT NULL,
    local_case_number character varying(100) NOT NULL,
	agency_name character varying(100) NOT NULL,
    acrs_report_type character varying(100) NOT NULL,
    crash_date_time character varying(100) NOT NULL,
    hit_run character varying(100) NOT NULL,
--     route_type character varying(100) NOT NULL,
--     mile_point character varying(100) NOT NULL,
--     mile_point_direction character varying(100) NOT NULL,
--     lane_direction character varying(100) NOT NULL,
--     lane_number character varying(100) NOT NULL,
    lane_type character varying(100) NOT NULL,
--     number_of_lanes character varying(100) NOT NULL,
--     direction character varying(100) NOT NULL,
--     distance character varying(100) NOT NULL,
--     distance_unit character varying(100) NOT NULL,
--     road_grade character varying(100) NOT NULL,
--     nontraffic character varying(100) NOT NULL,
--     road_name character varying(100) NOT NULL,
--     cross_street_type character varying(100) NOT NULL,
--     cross_street_name character varying(100) NOT NULL,
--     off_road_description character varying(100) NOT NULL,
--     municipality character varying(100) NOT NULL,
    related_non_motorist character varying(100) NOT NULL,
    at_fault character varying(100) NOT NULL,
    collision_type character varying(100) NOT NULL,
    weather character varying(100) NOT NULL,
    surface_condition character varying(100) NOT NULL,
    light character varying(100) NOT NULL,
--     traffic_control character varying(100) NOT NULL,
    driver_substance_abuse character varying(100) NOT NULL,
    non_motorist_substance_abuse character varying(100) NOT NULL,
--     first_harmful_event character varying(100) NOT NULL,
--     second_harmful_event character varying(100) NOT NULL,
    fixed_oject_struck character varying(100) NOT NULL,
--     junction character varying(100) NOT NULL,
--     intersection_type character varying(100) NOT NULL,
--     intersection_area character varying(100) NOT NULL,
--     road_alignment character varying(100) NOT NULL,
    road_condition character varying(100) NOT NULL,
--     road_division character varying(100) NOT NULL,
--     latitude character varying(100) NOT NULL,
--     longitude character varying(100) NOT NULL,
--     location character varying(100) NOT NULL,
-- 	PRIMARY KEY(IDNumber)
    CONSTRAINT pk_incidents PRIMARY KEY (report_number)
);


DROP TABLE IF EXISTS public.non_motorists_dimension;
CREATE TABLE public.non_motorists_dimension
(   IDNumber SERIAL PRIMARY KEY,
	report_number character varying(100) NOT NULL,
    local_case_number character varying(100) NOT NULL,
	agency_name character varying(100) NOT NULL,
    acrs_report_type character varying(100) NOT NULL,
    crash_date_time character varying(100) NOT NULL,
	route_type character varying(100) NOT NULL,
--     road_name character varying(100) NOT NULL,
--     cross_street_type character varying(100) NOT NULL,
--     cross_street_name character varying(100) NOT NULL,
    off_road_description character varying(100) NOT NULL,
--     municipality character varying(100) NOT NULL,
    related_non_motorist character varying(100) NOT NULL,
    collision_type character varying(100) NOT NULL,
    weather character varying(100) NOT NULL,
    surface_condition character varying(100) NOT NULL,
    light character varying(100) NOT NULL,
    traffic_control character varying(100) NOT NULL,
    driver_substance_abuse character varying(100) NOT NULL,
    non_motorist_substance_abuse character varying(100) NOT NULL,
    person_id character varying(100) NOT NULL,
    pedestrian_type character varying(100) NOT NULL,
    pedestrian_movement character varying(100) NOT NULL,
    pedestrian_actions character varying(100) NOT NULL,
    pedestrian_location character varying(100) NOT NULL,
    pedestrian_obeyed_traffic_signal character varying(100) NOT NULL,
    pedestrian_visibility character varying(100) NOT NULL,
    at_fault character varying(100) NOT NULL,
    injury_severity character varying(100) NOT NULL,
    safety_equipment character varying(100) NOT NULL
--     latitude character varying(100) NOT NULL,
--     longitude character varying(100) NOT NULL,
--     location character varying(100) NOT NULL,
-- 	PRIMARY KEY(IDNumber)
--     CONSTRAINT pk_non_motorists PRIMARY KEY (report_number)
);


DROP TABLE IF EXISTS public.final_table;
CREATE TABLE public.final_table
(  	IDNumber SERIAL PRIMARY KEY,
	report_number character varying(100) NOT NULL,
    non_motorists_report_number character varying(100) NOT NULL,
 
	drivers_report_number character varying(100) NOT NULL,
--     report_number_x character varying(100) NOT NULL,
    local_case_number character varying(100) NOT NULL,
    agency_name character varying(100) NOT NULL,
    acrs_report_type character varying(100) NOT NULL,
    crash_date_time character varying(100) NOT NULL,
    route_type character varying(100) NOT NULL,
--     road_name character varying(100) NOT NULL,
--     cross_street_type character varying(100) NOT NULL,
--     cross_street_name character varying(100) NOT NULL,
--     off_road_description character varying(100) NOT NULL,
--     municipality character varying(100) NOT NULL,
--     related_non_motorist character varying(100) NOT NULL,
    collision_type character varying(100) NOT NULL,
    weather character varying(100) NOT NULL,
    surface_condition character varying(100) NOT NULL,
    light character varying(100) NOT NULL,
    traffic_control character varying(100) NOT NULL,
    driver_substance_abuse character varying(100) NOT NULL,
    non_motorist_substance_abuse character varying(100) NOT NULL,
--     person_id character varying(100) NOT NULL,
    driver_at_fault character varying(100) NOT NULL,
    injury_severity character varying(100) NOT NULL,
--     circumstance character varying(100) NOT NULL,
    driver_distracted_by character varying(100) NOT NULL,
    drivers_license_state character varying(100) NOT NULL,
    vehicle_id character varying(100) NOT NULL,
    vehicle_damage_extent character varying(100) NOT NULL,
--     vehicle_first_impact_location character varying(100) NOT NULL,
--     vehicle_second_impact_location character varying(100) NOT NULL,
    vehicle_body_type character varying(100) NOT NULL,
    vehicle_movement character varying(100) NOT NULL,
--     vehicle_continuing_dir character varying(100) NOT NULL,
--     vehicle_going_dir character varying(100) NOT NULL,
    speed_limit character varying(100) NOT NULL,
    driverless_vehicle character varying(100) NOT NULL,
--     parked_vehicle character varying(100) NOT NULL,
    vehicle_year character varying(100) NOT NULL, 
    vehicle_make character varying(100) NOT NULL, 
    vehicle_model character varying(100) NOT NULL,
    equipment_problems character varying(100) NOT NULL,
	hit_run character varying(100) NOT NULL,
	lane_type character varying(100) NOT NULL,
	related_non_motorist character varying(100) NOT NULL,
    at_fault character varying(100) NOT NULL,
--     traffic_control character varying(100) NOT NULL,
--     first_harmful_event character varying(100) NOT NULL,
--     second_harmful_event character varying(100) NOT NULL,
    fixed_oject_struck character varying(100) NOT NULL,
--     junction character varying(100) NOT NULL,
--     intersection_type character varying(100) NOT NULL,
--     intersection_area character varying(100) NOT NULL,
--     road_alignment character varying(100) NOT NULL,
    road_condition character varying(100) NOT NULL,
--     road_name character varying(100) NOT NULL,
--     cross_street_type character varying(100) NOT NULL,
--     cross_street_name character varying(100) NOT NULL,
    off_road_description character varying(100) NOT NULL,
--     municipality character varying(100) NOT NULL,
    person_id character varying(100) NOT NULL,
    pedestrian_type character varying(100) NOT NULL,
    pedestrian_movement character varying(100) NOT NULL,
    pedestrian_actions character varying(100) NOT NULL,
    pedestrian_location character varying(100) NOT NULL,
    pedestrian_obeyed_traffic_signal character varying(100) NOT NULL,
    pedestrian_visibility character varying(100) NOT NULL,
    safety_equipment character varying(100) NOT NULL
);




ALTER TABLE IF EXISTS public.drivers_dimension  OWNER to dap;
ALTER TABLE IF EXISTS public.incidents_dimension  OWNER to dap;
ALTER TABLE IF EXISTS public.non_motorists_dimension  OWNER to dap;
ALTER TABLE IF EXISTS public.final_table  OWNER to dap;
ALTER FUNCTION public.extract_day_name(date)  OWNER TO dap;
ALTER FUNCTION public.extract_month_name(date) OWNER TO dap;
