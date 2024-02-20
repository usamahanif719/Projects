from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
mongo_connection_string = "mongodb://dap_project:dap_project@127.0.0.1"
print('hello')


DriversDataFrame = create_dagster_pandas_dataframe_type(
    name="DriversDataFrame",
    columns=[
        
        PandasColumn.string_column("report_number",non_nullable=True),
        PandasColumn.string_column("local_case_number", non_nullable=True),
        PandasColumn.string_column("agency_name", non_nullable=True),
        PandasColumn.string_column("acrs_report_type", non_nullable=True),
        PandasColumn.string_column("crash_date_time", non_nullable=True),
        PandasColumn.string_column("route_type", non_nullable=True),
        # PandasColumn.string_column("road_name", non_nullable=True),
        # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # PandasColumn.string_column("off_road_description", non_nullable=True),
        # PandasColumn.string_column("municipality", non_nullable=True),
        PandasColumn.string_column("collision_type", non_nullable=True),
        PandasColumn.string_column("weather", non_nullable=True),
        PandasColumn.string_column("surface_condition", non_nullable=True),
        PandasColumn.string_column("light", non_nullable=True),
        PandasColumn.string_column("traffic_control", non_nullable=True),
        PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        PandasColumn.string_column("person_id", non_nullable=True),
        PandasColumn.string_column("driver_at_fault", non_nullable=True),
        PandasColumn.string_column("injury_severity", non_nullable=True),
        # PandasColumn.string_column("circumstance", non_nullable=True),
        PandasColumn.string_column("driver_distracted_by", non_nullable=True),
        PandasColumn.string_column("vehicle_id", non_nullable=True),
        PandasColumn.string_column("vehicle_damage_extent", non_nullable=True),
        # PandasColumn.string_column("vehicle_first_impact_location", non_nullable=True),
        # PandasColumn.string_column("vehicle_second_impact_location", non_nullable=True),
        PandasColumn.string_column("vehicle_movement", non_nullable=True),
        # PandasColumn.string_column("vehicle_continuing_dir", non_nullable=True),
        # PandasColumn.string_column("vehicle_going_dir", non_nullable=True),
        PandasColumn.string_column("speed_limit", non_nullable=True),
        PandasColumn.string_column("driverless_vehicle", non_nullable=True),
        # PandasColumn.string_column("parked_vehicle", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_a9cs_3ed7", non_nullable=True),
        PandasColumn.string_column("vehicle_year", non_nullable=True),
        PandasColumn.string_column("vehicle_make", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_r648_kzwt", non_nullable=True),
        PandasColumn.string_column("vehicle_model", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_vu5j_pcmz", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_tx5f_5em3", non_nullable=True),
        PandasColumn.string_column("equipment_problems", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_kbsp_ykn9", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_d7bw_bq6x", non_nullable=True),
        # PandasColumn.string_column("longitude", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_rbt8_3x7n", non_nullable=True),
        # PandasColumn.string_column("geolocation", non_nullable=True),
        PandasColumn.string_column("drivers_license_state", non_nullable=True),
        PandasColumn.string_column("vehicle_body_type", non_nullable=True),
        # PandasColumn.string_column("off_road_description", non_nullable=True),
        # PandasColumn.string_column("related_non_motorist", non_nullable=True),
        PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True)
        










        # PandasColumn.string_column("report_number",non_nullable=True, unique=True),
        # PandasColumn.string_column("local_case_number", non_nullable=True),
        # PandasColumn.string_column("agency_name", non_nullable=True),
        # PandasColumn.string_column("acrs_report_type", non_nullable=True),
        # PandasColumn.string_column("crash_date_time", non_nullable=True),
        # PandasColumn.string_column("route_type", non_nullable=True),
        # # PandasColumn.string_column("road_name", non_nullable=True),
        # # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # # PandasColumn.string_column("off_road_description", non_nullable=True),
        # # PandasColumn.string_column("municipality", non_nullable=True),
        # PandasColumn.string_column("related_non_motorist", non_nullable=True),
        # PandasColumn.string_column("collision_type", non_nullable=True),
        # PandasColumn.string_column("weather", non_nullable=True),
        # PandasColumn.string_column("surface_condition", non_nullable=True),
        # PandasColumn.string_column("light", non_nullable=True),
        # PandasColumn.string_column("traffic_control", non_nullable=True),
        # PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("person_id", non_nullable=True),
        # PandasColumn.string_column("driver_at_fault", non_nullable=True),
        # PandasColumn.string_column("injury_severity", non_nullable=True),
        # # PandasColumn.string_column("circumstance", non_nullable=True),
        # PandasColumn.string_column("driver_distracted_by", non_nullable=True),
        # PandasColumn.string_column("drivers_license_state", non_nullable=True),
        # PandasColumn.string_column("vehicle_id", non_nullable=True),
        # PandasColumn.string_column("vehicle_damage_extent", non_nullable=True),
        # # PandasColumn.string_column("vehicle_first_impact_location", non_nullable=True),
        # # PandasColumn.string_column("vehicle_second_impact_location", non_nullable=True),
        # PandasColumn.string_column("vehicle_body_type", non_nullable=True),
        # PandasColumn.string_column("vehicle_movement", non_nullable=True),
        # # PandasColumn.string_column("vehicle_continuing_dir", non_nullable=True),
        # # PandasColumn.string_column("vehicle_going_dir", non_nullable=True),
        # PandasColumn.string_column("speed_limit", non_nullable=True),
        # PandasColumn.string_column("driverless_vehicle", non_nullable=True),
        # # PandasColumn.string_column("parked_vehicle", non_nullable=True),
        # PandasColumn.string_column("vehicle_year", non_nullable=True),
        # PandasColumn.string_column("vehicle_make", non_nullable=True),
        # PandasColumn.string_column("vehicle_model", non_nullable=True),
        # PandasColumn.string_column("equipment_problems", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # # PandasColumn.string_column("longitude", non_nullable=True),
        # # PandasColumn.string_column("geolocation", non_nullable=True)
    ],
)

IncidentsDataFrame = create_dagster_pandas_dataframe_type(
    name="IncidentsDataFrame",
    columns=[
        PandasColumn.string_column("report_number",non_nullable=True, unique=True),
        PandasColumn.string_column("local_case_number", non_nullable=True),
        PandasColumn.string_column("agency_name", non_nullable=True),
        PandasColumn.string_column("acrs_report_type", non_nullable=True),
        PandasColumn.string_column("crash_date_time", non_nullable=True),
        PandasColumn.string_column("hit_run", non_nullable=True),
        # PandasColumn.string_column("route_type", non_nullable=True),
        # PandasColumn.string_column("mile_point", non_nullable=True),
        # PandasColumn.string_column("mile_point_direction", non_nullable=True),
        # PandasColumn.string_column("lane_direction", non_nullable=True),
        # PandasColumn.string_column("lane_number", non_nullable=True),
        # PandasColumn.string_column("number_of_lanes", non_nullable=True),
        # PandasColumn.string_column("direction", non_nullable=True),
        # PandasColumn.string_column("distance", non_nullable=True),
        # PandasColumn.string_column("distance_unit", non_nullable=True),
        # PandasColumn.string_column("road_grade", non_nullable=True),
        # PandasColumn.string_column("nontraffic", non_nullable=True),
        # PandasColumn.string_column("road_name", non_nullable=True),
        # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # PandasColumn.string_column("municipality", non_nullable=True),
        PandasColumn.string_column("at_fault", non_nullable=True),
        PandasColumn.string_column("collision_type", non_nullable=True),
        PandasColumn.string_column("weather", non_nullable=True),
        PandasColumn.string_column("surface_condition", non_nullable=True),
        PandasColumn.string_column("light", non_nullable=True),
        # PandasColumn.string_column("traffic_control", non_nullable=True),
        PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("first_harmful_event", non_nullable=True),
        # PandasColumn.string_column("second_harmful_event", non_nullable=True),
        PandasColumn.string_column("fixed_oject_struck", non_nullable=True),
        # PandasColumn.string_column("junction", non_nullable=True),
        # PandasColumn.string_column("intersection_type", non_nullable=True),
        # PandasColumn.string_column("intersection_area", non_nullable=True),
        # PandasColumn.string_column("road_alignment", non_nullable=True),
        PandasColumn.string_column("road_condition", non_nullable=True),
        # PandasColumn.string_column("road_division", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # PandasColumn.string_column("longitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/latitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/longitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/human_address", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_vu5j_pcmz", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_tx5f_5em3", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_kbsp_ykn9", non_nullable=True),
#  /       PandasColumn.string_column(":@computed_region_d7bw_bq6x", non_nullable=True),
# /        PandasColumn.string_column(":@computed_region_rbt8_3x7n", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_a9cs_3ed7", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_r648_kzwt", non_nullable=True),
        # PandasColumn.string_column("off_road_description", non_nullable=True),
        PandasColumn.string_column("lane_type", non_nullable=True),
        PandasColumn.string_column("related_non_motorist", non_nullable=True),
        PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True),





        # PandasColumn.string_column("report_number",non_nullable=True, unique=True),
        # PandasColumn.string_column("local_case_number", non_nullable=True),
        # PandasColumn.string_column("agency_name", non_nullable=True),
        # PandasColumn.string_column("acrs_report_type", non_nullable=True),
        # PandasColumn.string_column("crash_date_time", non_nullable=True),
        # PandasColumn.string_column("hit_run", non_nullable=True),
        # # PandasColumn.string_column("route_type", non_nullable=True),
        # # PandasColumn.string_column("mile_point", non_nullable=True),
        # # PandasColumn.string_column("mile_point_direction", non_nullable=True),
        # # PandasColumn.string_column("lane_direction", non_nullable=True),
        # # PandasColumn.string_column("lane_number", non_nullable=True),
        # # PandasColumn.string_column("lane_type", non_nullable=True),
        # # PandasColumn.string_column("number_of_lanes", non_nullable=True),
        # # PandasColumn.string_column("direction", non_nullable=True),
        # # PandasColumn.string_column("distance", non_nullable=True),
        # # PandasColumn.string_column("distance_unit", non_nullable=True),
        # # PandasColumn.string_column("road_grade", non_nullable=True),
        # PandasColumn.string_column("nontraffic", non_nullable=True),
        # # PandasColumn.string_column("road_name", non_nullable=True),
        # # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # # PandasColumn.string_column("off_road_description", non_nullable=True),
        # # PandasColumn.string_column("municipality", non_nullable=True),
        # PandasColumn.string_column("related_non_motorist", non_nullable=True),
        # PandasColumn.string_column("at_fault", non_nullable=True),
        # PandasColumn.string_column("collision_type", non_nullable=True),
        # PandasColumn.string_column("weather", non_nullable=True),
        # PandasColumn.string_column("surface_condition", non_nullable=True),
        # PandasColumn.string_column("light", non_nullable=True),
        # # PandasColumn.string_column("traffic_control", non_nullable=True),
        # PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True),
        # # PandasColumn.string_column("first_harmful_event", non_nullable=True),
        # # PandasColumn.string_column("second_harmful_event", non_nullable=True),
        # # PandasColumn.string_column("fixed_object_struck", non_nullable=True),
        # # PandasColumn.string_column("junction", non_nullable=True),
        # # PandasColumn.string_column("intersection_type", non_nullable=True),
        # # PandasColumn.string_column("intersection_area", non_nullable=True),
        # # PandasColumn.string_column("road_alignment", non_nullable=True),
        # # PandasColumn.string_column("road_condition", non_nullable=True),
        # # PandasColumn.string_column("road_division", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # # PandasColumn.string_column("longitude", non_nullable=True),
        # PandasColumn.string_column("geolocation", non_nullable=True)
    ],
)



Non_MotoristsDataFrame = create_dagster_pandas_dataframe_type(
    name="Non_MotoristsDataFrame",
    columns=[
       PandasColumn.string_column("report_number",non_nullable=True),
        PandasColumn.string_column("local_case_number", non_nullable=True),
        PandasColumn.string_column("agency_name", non_nullable=True),
        PandasColumn.string_column("acrs_report_type", non_nullable=True),
        PandasColumn.string_column("crash_date_time", non_nullable=True),
        PandasColumn.string_column("off_road_description", non_nullable=True),
        PandasColumn.string_column("related_non_motorist", non_nullable=True),
        PandasColumn.string_column("collision_type", non_nullable=True),
        PandasColumn.string_column("weather", non_nullable=True),
        PandasColumn.string_column("light", non_nullable=True),
        PandasColumn.string_column("traffic_control", non_nullable=True),
        PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True),
        PandasColumn.string_column("person_id", non_nullable=True),
        PandasColumn.string_column("pedestrian_type", non_nullable=True),
        PandasColumn.string_column("pedestrian_movement", non_nullable=True),
        PandasColumn.string_column("pedestrian_actions", non_nullable=True),
        PandasColumn.string_column("pedestrian_location", non_nullable=True),
        PandasColumn.string_column("pedestrian_obeyed_traffic_signal", non_nullable=True),
        PandasColumn.string_column("pedestrian_visibility", non_nullable=True),
        PandasColumn.string_column("at_fault", non_nullable=True),
        PandasColumn.string_column("injury_severity", non_nullable=True),
        PandasColumn.string_column("safety_equipment", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # PandasColumn.string_column("longitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/latitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/longitude", non_nullable=True),
        # PandasColumn.string_column("geolocation/human_address", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_vu5j_pcmz", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_tx5f_5em3", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_kbsp_ykn9", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_d7bw_bq6x", non_nullable=True),
#  /       PandasColumn.string_column(":@computed_region_rbt8_3x7n", non_nullable=True),
# /        PandasColumn.string_column(":@computed_region_a9cs_3ed7", non_nullable=True),
        # PandasColumn.string_column(":@computed_region_r648_kzwt", non_nullable=True),
        PandasColumn.string_column("route_type", non_nullable=True),
        # PandasColumn.string_column("road_name", non_nullable=True),
        # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # PandasColumn.string_column("municipality", non_nullable=True),
        PandasColumn.string_column("surface_condition", non_nullable=True),

      
      
      
      
      
        # PandasColumn.string_column("report_number",non_nullable=True, unique=True),
        # PandasColumn.string_column("local_case_number", non_nullable=True),
        # PandasColumn.string_column("agency_name", non_nullable=True),
        # PandasColumn.string_column("acrs_report_type", non_nullable=True),
        # PandasColumn.string_column("crash_date_time", non_nullable=True),
        # PandasColumn.string_column("route_type", non_nullable=True),
        # PandasColumn.string_column("road_name", non_nullable=True),
        # PandasColumn.string_column("cross_street_type", non_nullable=True),
        # PandasColumn.string_column("cross_street_name", non_nullable=True),
        # PandasColumn.string_column("off_road_description", non_nullable=True),
        # PandasColumn.string_column("municipality", non_nullable=True),
        # PandasColumn.string_column("related_non_motorist", non_nullable=True),
        # PandasColumn.string_column("collision_type", non_nullable=True),
        # PandasColumn.string_column("weather", non_nullable=True),
        # PandasColumn.string_column("surface_condition", non_nullable=True),
        # PandasColumn.string_column("light", non_nullable=True),
        # PandasColumn.string_column("traffic_control", non_nullable=True),
        # PandasColumn.string_column("driver_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("non_motorist_substance_abuse", non_nullable=True),
        # PandasColumn.string_column("person_id", non_nullable=True),
        # PandasColumn.string_column("pedestrian_type", non_nullable=True),
        # PandasColumn.string_column("pedestrian_movement", non_nullable=True),
        # PandasColumn.string_column("pedestrian_actions", non_nullable=True),
        # PandasColumn.string_column("pedestrian_location", non_nullable=True),
        # PandasColumn.string_column("pedestrian_obeyed_traffic_signal", non_nullable=True),
        # PandasColumn.string_column("pedestrian_visibility", non_nullable=True),
        # PandasColumn.string_column("at_fault", non_nullable=True),
        # PandasColumn.string_column("injury_severity", non_nullable=True),
        # PandasColumn.string_column("safety_equipment", non_nullable=True),
        # PandasColumn.string_column("latitude", non_nullable=True),
        # PandasColumn.string_column("longitude", non_nullable=True),
        # PandasColumn.string_column("location", non_nullable=True)
    ],
)

def is_tuple(_, value):
    return isinstance(value, tuple) and all(
        isinstance(element, datetime) for element in value
    )

DateTuple = DagsterType(
    name="DateTuple",
    type_check_fn=is_tuple,
    description="A tuple of scalar values",
)
print('done')


incidents_columns = {
       "Report Number" : "report_number",
       "Local Case Number" :"local_case_number",
        "Agency Name":"agency_name",
        "ACRS Report Type":"acrs_report_type",
        "Crash Date/Time":"crash_date_time",
        "Hit/Run":"hit_run",
        "Route Type":"route_type",
        "Mile Point": "mile_point",
        "Mile Point Direction":"mile_point_direction",
        "Lane Direction":"lane_direction",
        "Lane Number":"lane_number",
        "Lane Type":"lane_type",
        "Number of Lanes":"number_of_lanes",
        "Direction":"direction",
        "Distance":"distance",
        "Distance Unit": "distance_unit",
        "Road Grade":"road_grade",
        "NonTraffic":"non_traffic",
        "Road Name":"road_name",
        "Cross-Street Type":"cross_street_type",
        "Cross-Street Name":"cross_street_name",
        "Off-Road Description":"off_road_description",
        "Municipality":"municipality",
        "Related Non-Motorist":"related_non_motorist",
        "At Fault":"at_fault",
        "Collision Type":"collision_type",
        "Weather":"weather",
        "Surface Condition":"surface_condition",
        "Light":"light",
        "Traffic Control":"traffic_control",
        "Driver Substance Abuse":"driver_substance_abuse",
        "Non-Motorist Substance_abuse":"non_motorist_substance_abuse",
        "First Harmful Event":"first_harmful_event",
        "Second Harmful Event":"second_harmful_event",
        "Fixed Oject Struck":"fixed_object_struck",
        "Junction":"junction",
        "Intersection Type":"intersection_type",
        "Intersection Area":"intersection_area",
        "Road Alignment":"road_alignment",
        "Road Condition":"road_condition",
        "Road Division":"road_division",
        "Latitude":"latitude",
        "Longitude":"longitude",
        "Location":"location",
}



drivers_columns = {
        "Report Number" : "report_number",
        "Local Case Number" :"local_case_number",
        "Agency Name":"agency_name",
        "ACRS Report Type":"acrs_report_type",
        "Crash Date/Time":"crash_date_time",
        "Route Type":"route_type",
        "Road Name":"road_name",
        "Cross-Street Type":"cross_street_type",
        "Cross-Street Name":"cross_street_name",
        "Off-Road Description":"off_road_description",
        "Municipality":"municipality",
        "Related Non-Motorist":"related_non_motorist",
        "Collision Type":"collision_type",
        "Weather":"weather",
        "Surface Condition":"surface_condition",
        "Light":"light",
        "Traffic Control":"traffic_control",
        "Driver Substance Abuse":"driver_substance_abuse",
        "Non-Motorist Substance_abuse":"non_motorist_substance_abuse",
        "Person ID":"person_id",
        "Driver At Fault":"driver_at_fault",
        "Injury Severity":"injury_severity",
        "Circumstance":"circumstance",
        "Driver Distracted By":"driver_distracted_by",
        "Driver License State":"drivers_license_state",
        "Vehicle ID":"vehicle_id",
        "Vehicle Damage Extent":"vehicle_damage_extent",
        "Vehicle First Impact Location":"vehicle_first_impact_location",
        "Vehicle Second Impact Location":"vehicle_second_impact_location",
        "Vehicle Body Type":"vehicle_body_type",
        "Vehicle Movement":"vehicle_movement",
        "Vehicle Continuing Dir":"vehicle_continuing_dir",
        "Vehicle Going Dir":"vehicle_going_dir",
        "Speed Limit":"speed_limit",
        "Driverless Vehicle":"driverless_vehicle",
        "Parked Vehicle":"parked_vehicle",
        "Vehicle Year":"vehicle_year", 
        "Vehicle Make":"vehicle_make", 
        "Vehicle Model":"vehicle_model",
        "Equipment Problems":"equipment_problems",
        "Latitude":"latitude",
        "Longitude":"longitude",
        "Location":"location",
}


non_motorists_columns = {
       "Report Number" : "report_number",
       "Local Case Number" :"local_case_number",
        "Agency Name":"agency_name",
        "ACRS Report Type":"acrs_report_type",
        "Crash Date/Time":"crash_date_time",
        "Route Type":"route_type",
        "Road Name":"road_name",
        "Cross-Street Type":"cross_street_type",
        "Cross-Street Name":"cross_street_name",
        "Off-Road Description":"off_road_description",
        "Municipality":"municipality",
        "Related Non-Motorist":"related_non_motorist",
        "Collision Type":"collision_type",
        "Weather":"weather",
        "Surface Condition":"surface_condition",
        "Light":"light",
        "Traffic Control":"traffic_control",
        "Driver Substance Abuse":"driver_substance_abuse",
        "Non-Motorist Substance_abuse":"non_motorist_substance_abuse",
        "Person ID":"person_id",
        "Pedestrain Type":"pedestrian_type",
        "Pedestrain Movement":"pedestrian_movement",
        "Pedestrain Actions":"pedestrian_actions",
        "Pedestrain Location":"pedestrian_location",
        "Pedestrain Obeyed Traffic Signal":"pedestrian_obeyed_traffic_signal",
        "Pedestrain Visibility":"pedestrian_visibility",
        "At Fault":"at_fault",
        "Injury Severity":"injury_severity",
        "Safety Equipment":"safety_equipment",
        "Latitude":"latitude",
        "Longitude":"longitude",
        "Location":"location",
}


@op(ins={'start': In(bool)}, out=Out(DriversDataFrame))
def extract_drivers(start) -> DriversDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["dap_project_database"]
    Drivers_Data = pd.DataFrame(db.Drivers_Data.find({}))
    Drivers_Data.drop(
      columns=["_id",":@computed_region_a9cs_3ed7",":@computed_region_d7bw_bq6x",":@computed_region_kbsp_ykn9",
        ":@computed_region_r648_kzwt",":@computed_region_rbt8_3x7n",":@computed_region_tx5f_5em3",":@computed_region_vu5j_pcmz",
        "circumstance","cross_street_name","cross_street_type",
        "geolocation","latitude","longitude","municipality",
        "off_road_description","parked_vehicle","related_non_motorist","road_name",
        "vehicle_continuing_dir","vehicle_first_impact_location","vehicle_going_dir","vehicle_second_impact_location"
],


        # columns=["geolocation", "longitude","parked_vehicle","vehicle_going_dir","vehicle_continuing_dir", "vehicle_second_impact_location",
        # "vehicle_first_impact_location","circumstance","municipality", "off_road_description",
        # "cross_street_name", "cross_street_type","road_name"],
        axis=1,
        inplace=True
    )
    # Drivers_Data = Drivers_Data.fillna(' ', inplace=True)
    # Drivers_Data[["report_number","local_case_number","agency_name","acrs_report_type","crash_date_time","route_type","road_name","cross_street_type",
    # "cross_street_name","municipality","collision_type","weather","surface_condition","light","traffic_control","driver_substance_abuse",
    # "person_id","driver_at_fault","injury_severity","circumstance","driver_distracted_by","vehicle_id","vehicle_damage_extent","vehicle_first_impact_location",
    # "vehicle_second_impact_location","vehicle_movement","vehicle_continuing_dir","vehicle_going_dir","speed_limit","driverless_vehicle","parked_vehicle",
    # ":@computed_region_a9cs_3ed7","vehicle_make","vehicle_year",":@computed_region_r648_kzwt","vehicle_model",":@computed_region_vu5j_pcmz",":@computed_region_tx5f_5em3",
    # "equipment_problems","latitude",":@computed_region_kbsp_ykn9",":@computed_region_d7bw_bq6x","longitude",":@computed_region_rbt8_3x7n","geolocation"]] = Drivers_Data[["report_number","local_case_number","agency_name","acrs_report_type","crash_date_time","route_type","road_name","cross_street_type",
    # "cross_street_name","municipality","collision_type","weather","surface_condition","light","traffic_control","driver_substance_abuse",
    # "person_id","driver_at_fault","injury_severity","circumstance","driver_distracted_by","vehicle_id","vehicle_damage_extent","vehicle_first_impact_location",
    # "vehicle_second_impact_location","vehicle_movement","vehicle_continuing_dir","vehicle_going_dir","speed_limit","driverless_vehicle","parked_vehicle",
    # ":@computed_region_a9cs_3ed7","vehicle_make","vehicle_year",":@computed_region_r648_kzwt","vehicle_model",":@computed_region_vu5j_pcmz",":@computed_region_tx5f_5em3",
    # "equipment_problems","latitude",":@computed_region_kbsp_ykn9",":@computed_region_d7bw_bq6x","longitude",":@computed_region_rbt8_3x7n","geolocation",
    # ]].fillna('UNKNOWN')



    # Drivers_Data[['report_number', 'local_case_number', 'agency_name', 'acrs_report_type', 'crash_date_time','route_type',
    # 'collision_type','weather', 'surface_condition', 'light','traffic_control', 'driver_substance_abuse','person_id', 'driver_at_fault','injury_severity',
    # 'vehicle_id','vehicle_damage_extent','vehicle_year','vehicle_make','vehicle_model','equipment_problems', 'vehicle_body_type']] = Drivers_Data[['report_number', 'local_case_number', 'agency_name', 'acrs_report_type', 'crash_date_time','route_type',
    # 'collision_type','weather', 'surface_condition', 'light','traffic_control', 'driver_substance_abuse','person_id', 'driver_at_fault','injury_severity',
    # 'vehicle_id','vehicle_damage_extent','vehicle_year','vehicle_make','vehicle_model','equipment_problems', 'vehicle_body_type']].fillna('UNKNOWN')
    # Drivers_Data.interpolate(method ='linear', limit_direction ='forward', inplace=True)
    # for col in Drivers_Data:
    #     Drivers_Data[col] = Drivers_Data[col].fillna('UNKNOWN')
    #     print(col)
    # Drivers_Data.fillna(method='ffill', inplace=True)
    # Drivers_Data = Drivers_Data.dropna()
    # Drivers_Data = Drivers_Data['report_number'].drop_duplicates()
    # Drivers_Data.rename(
    #     columns=drivers_columns,
    #     inplace=True
    # )
    Drivers_Data = Drivers_Data.astype(str)
    conn.close()
    return Drivers_Data


@op(ins={'Drivers_Data': In(DriversDataFrame)}, out=Out(None))
def stage_extracted_Drivers_Data(Drivers_Data):
    Drivers_Data.to_csv("staging/drivers.csv",index=False,sep="\t")





@op(ins={'start': In(bool)}, out=Out(IncidentsDataFrame))
def extract_incidents(start) -> IncidentsDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["dap_project_database"]
    Incidents_Data = pd.DataFrame(db.Incidents_Data.find({}))
    Incidents_Data.drop(
         columns=[ "_id",":@computed_region_a9cs_3ed7",":@computed_region_d7bw_bq6x",":@computed_region_kbsp_ykn9",":@computed_region_r648_kzwt",
        ":@computed_region_rbt8_3x7n",":@computed_region_tx5f_5em3",":@computed_region_vu5j_pcmz","cross_street_name","cross_street_type",
        "direction","distance","distance_unit","geolocation",
        "intersection_area","intersection_type","junction","lane_direction","lane_number","latitude",
        "longitude","mile_point","mile_point_direction","municipality","nontraffic","number_of_lanes",
        "off_road_description","road_alignment","road_division","road_grade","road_name","route_type",
        "second_harmful_event","first_harmful_event","traffic_control"],
       
        
        
        
        
        # columns=[ "longitude","road_alignment","intersection_area","intersection_type", "junction","first_harmful_event",
        # "second_harmful_event","municipality", "off_road_description","traffic_control","cross_street_name", "cross_street_type","road_name", 
        # "road_grade", "distance", "distance_unit", "direction", "number_of_lanes", "lane_type", "lane_number","lane_direction","mile_point",
        # "mile_point_direction", "route_type"],
        axis=1,
        inplace=True
    )
    # Incidents_Data[['report_number', 'local_case_number', 'agency_name', 'acrs_report_type', 'crash_date_time','hit_run','nontraffic','at_fault',
    # 'collision_type','weather', 'surface_condition', 'light', 'driver_substance_abuse','lane_type','related_non_motorist','non_motorist_substance_abuse']] = Incidents_Data[['report_number', 'local_case_number', 'agency_name', 'acrs_report_type', 'crash_date_time','hit_run','nontraffic','at_fault',
    # 'collision_type','weather', 'surface_condition', 'light', 'driver_substance_abuse','lane_type','related_non_motorist','non_motorist_substance_abuse']].fillna('UNKNOWN')

    # Incidents_Data = Incidents_Data.dropna()
    # Incidents_Data.rename(
    #     columns=incidents_columns,
    #     inplace=True
    # )
    Incidents_Data = Incidents_Data.astype(str)
    conn.close()
    return Incidents_Data

@op(ins={'Incidents_Data': In(IncidentsDataFrame)}, out=Out(None))
def stage_extracted_Incidents_Data(Incidents_Data):
    Incidents_Data.to_csv("staging/incidents.csv",index=False,sep="\t")
    





@op(ins={'start': In(bool)}, out=Out(Non_MotoristsDataFrame))
def extract_non_motorists(start) -> Non_MotoristsDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["dap_project_database"]
    Non_Motorists_Data = pd.DataFrame(db.Non_Motorists_Data.find({}))
    Non_Motorists_Data.drop(
        columns=["_id",":@computed_region_a9cs_3ed7",":@computed_region_d7bw_bq6x",":@computed_region_kbsp_ykn9",":@computed_region_r648_kzwt",
        ":@computed_region_rbt8_3x7n",":@computed_region_tx5f_5em3",":@computed_region_vu5j_pcmz","cross_street_name",
        "geolocation","latitude","longitude","municipality","cross_street_type",
        "road_name"],
       
       
       


        # columns=["longitude","traffic_control","municipality", "off_road_description",
        # "cross_street_name", "cross_street_type","road_name","surface_condition","route_type",":@computed_region_r648_kzwt",":@computed_region_rbt8_3x7n",":@computed_region_d7bw_bq6x",":@computed_region_a9cs_3ed7"],
        axis=1,
        inplace=True
    )
    # Non_Motorists_Data = Non_Motorists_Data.dropna(subset=['@computed_region_a9cs_3ed7'])
    # Non_Motorists_Data = Non_Motorists_Data.dropna()
    # Non_Motorists_Data.rename(
    #     columns=non_motorists_columns,
    #     inplace=True
    # )
    Non_Motorists_Data = Non_Motorists_Data.astype(str)
    conn.close()
    return Non_Motorists_Data


@op(ins={'Non_Motorists_Data': In(Non_MotoristsDataFrame)}, out=Out(None))
def stage_extracted_Non_Motorists_Data(Non_Motorists_Data):
    Non_Motorists_Data.to_csv("staging/non_motorists.csv",index=False,sep="\t")

