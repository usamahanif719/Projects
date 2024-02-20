from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd

postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/Reporting"


@op(ins={'start': In(None)},out=Out(bool))
def load_drivers_dimension(start):
    logger = get_dagster_logger()
    drivers = pd.read_csv("staging/drivers.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE drivers_dimension;")
        rowcount = drivers.to_sql(
            name="drivers_dimension",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False




@op(ins={'start': In(None)},out=Out(bool))
def load_incidents_dimension(start):
    logger = get_dagster_logger()
    incidents = pd.read_csv("staging/incidents.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE incidents_dimension;")
        rowcount = incidents.to_sql(
            name="incidents_dimension",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


@op(ins={'start': In(None)},out=Out(bool))
def load_non_motorists_dimension(start):
    logger = get_dagster_logger()
    non_motorists = pd.read_csv("staging/non_motorists.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE non_motorists_dimension;")
        rowcount = non_motorists.to_sql(
            name="non_motorists_dimension",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False

@op(ins={'start': In(None)}, out=Out(None))
def load_final_table(start):
    logger = get_dagster_logger()
    if start:
        incidents = pd.read_csv(
            "staging/incidents.csv",
            sep="\t",
        )
        try:
            engine = create_engine(postgres_connection_string)

            # load drivers dimension data
            drivers = pd.read_sql(
                "SELECT  report_number as drivers_report_number, local_case_number,agency_name,acrs_report_type,crash_date_time,route_type,collision_type,weather,surface_condition, light, traffic_control, driver_substance_abuse,non_motorist_substance_abuse,person_id,driver_at_fault,injury_severity,driver_distracted_by,drivers_license_state,vehicle_id,vehicle_damage_extent,vehicle_body_type,vehicle_movement,speed_limit,driverless_vehicle,vehicle_year,vehicle_make,vehicle_model,equipment_problems  FROM drivers_dimension",
                con=engine
            )
            # load incidents dimension data
            incidents = pd.read_sql(
                "SELECT report_number, road_condition,hit_run,lane_type,related_non_motorist,at_fault,fixed_oject_struck   FROM incidents_dimension",
                con=engine
            )
            # join to incidents dimension to drivers dimension
            incidents = incidents.merge(
                drivers,
                how="inner",
                left_on="report_number",
                right_on="drivers_report_number")

            # load incidents dimension data
            non_motorists = pd.read_sql(
                "SELECT report_number as non_motorists_report_number , off_road_description ,    pedestrian_movement,    pedestrian_type  ,pedestrian_actions,pedestrian_location,pedestrian_obeyed_traffic_signal, pedestrian_visibility, safety_equipment   FROM non_motorists_dimension",
                con=engine
            )
            
            # join to incidents dimension to non_motorists dimension
            incidents = incidents.merge(
                non_motorists,
                how="inner",
                left_on="report_number",
                right_on="non_motorists_report_number")


            # logger = get_dagster_logger()
            engine = create_engine(postgres_connection_string, poolclass=NullPool)
            engine.execute("TRUNCATE public.final_table;")
            rowcount = incidents.to_sql(
                name="final_table",
                schema="public",
                con=engine,
                index=False,
                if_exists="append"
            )
            logger.info("%i final records loaded" % rowcount)
            engine.dispose(close=True)
            return rowcount > 0
        except exc.SQLAlchemyError as error:
            logger.error("Error: %s" % error)


