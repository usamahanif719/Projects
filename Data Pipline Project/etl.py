import webbrowser
from dagster import job
from Extract import *
from Load import *

@op(out=Out(bool))
def load_dimensions(driversdim, incidentsdim, non_motoristsdim):
    return driversdim and incidentsdim and non_motoristsdim


@job
def etl():
    load_final_table(    
        load_dimensions(                        
            driversdim=load_drivers_dimension(
                stage_extracted_Drivers_Data(
                    extract_drivers()
                                )
                            ),
            incidentsdim=load_incidents_dimension(                                
                stage_extracted_Incidents_Data(
                    extract_incidents()
                                )
                            ),
            non_motoristsdim=load_non_motorists_dimension(
                stage_extracted_Non_Motorists_Data(
                    extract_non_motorists()
                                )
                            )
                                )
    
    )