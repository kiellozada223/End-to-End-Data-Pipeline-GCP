import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df = data.drop_duplicates().reset_index(drop=True)
    df['ID'] = df.index
    df = df[['ID','hvfhs_license_num','dispatching_base_num','originating_base_num','request_datetime','on_scene_datetime','pickup_datetime',
            'dropoff_datetime','PULocationID','DOLocationID','trip_miles','trip_time','base_passenger_fare','tolls','bcf','sales_tax','congestion_surcharge',
            'airport_fee','tips','driver_pay','shared_request_flag','shared_match_flag','access_a_ride_flag','wav_request_flag','wav_match_flag']].reset_index(drop=True)
            
    df['request_datetime'] = pd.to_datetime(df['request_datetime'])
    df['on_scene_datetime'] = pd.to_datetime(df['on_scene_datetime'])
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] =pd.to_datetime(df['dropoff_datetime'])
    
    datetime_dim = df[['request_datetime', 'on_scene_datetime','pickup_datetime','dropoff_datetime']].reset_index(drop=True)

    datetime_dim['request_minute'] = datetime_dim['request_datetime'].dt.minute
    datetime_dim['request_hour'] = datetime_dim['request_datetime'].dt.hour
    datetime_dim['request_day'] = datetime_dim['request_datetime'].dt.day
    datetime_dim['request_month'] = datetime_dim['request_datetime'].dt.month
    datetime_dim['request_year'] = datetime_dim['request_datetime'].dt.year

    datetime_dim['on_scene_minute'] = datetime_dim['on_scene_datetime'].dt.minute
    datetime_dim['on_scene_hour'] = datetime_dim['on_scene_datetime'].dt.hour
    datetime_dim['on_scene_day'] = datetime_dim['on_scene_datetime'].dt.day
    datetime_dim['on_scene_month'] = datetime_dim['on_scene_datetime'].dt.month
    datetime_dim['on_scene_year'] = datetime_dim['on_scene_datetime'].dt.year

    datetime_dim['pickup_minute'] = datetime_dim['pickup_datetime'].dt.minute
    datetime_dim['pickup_hour'] = datetime_dim['pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim['pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim['pickup_datetime'].dt.year

    datetime_dim['dropoff_minute'] = datetime_dim['dropoff_datetime'].dt.minute
    datetime_dim['dropoff_hour'] = datetime_dim['dropoff_datetime'].dt.hour
    datetime_dim['dropoff_day'] = datetime_dim['dropoff_datetime'].dt.day
    datetime_dim['dropoff_month'] = datetime_dim['dropoff_datetime'].dt.month
    datetime_dim['dropoff_year'] = datetime_dim['dropoff_datetime'].dt.year

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[['datetime_id', 'request_datetime', 'request_minute', 'request_hour', 'request_day', 'request_month', 'request_year',
                                'on_scene_datetime','on_scene_minute', 'on_scene_hour','on_scene_day','on_scene_month','on_scene_year',
                                'pickup_datetime','pickup_minute','pickup_hour','pickup_day','pickup_month','pickup_year',
                                'dropoff_datetime','dropoff_minute','dropoff_hour','dropoff_day','dropoff_month','dropoff_year']].reset_index(drop=True)# Specify your transformation logic here

    service_name = {
    'HV0002':'Juno',
    'HV0003':'Uber',
    'HV0004':'Via',
    'HV0005':'Lyft'
    }

    service_license_dim = pd.DataFrame({
        'hvfhs_license_num':df['hvfhs_license_num'],
        'originating_base_num':df['originating_base_num'],
        'dispatching_base_num':df['dispatching_base_num'],
        'service_provider':df['hvfhs_license_num'].map(service_name)
    })

    service_license_dim['service_id'] = service_license_dim.index

    service_license_dim = service_license_dim[['service_id','hvfhs_license_num','originating_base_num',
                                            'dispatching_base_num', 'service_provider']].reset_index(drop=True)

    
    PULocation_dim = pd.merge(df['PULocationID'],args[0], how='left', left_on='PULocationID', right_on='LocationID')
    PULocation_dim = PULocation_dim[['PULocationID', 'Borough', 'Zone']].reset_index(drop=True)
    PULocation_dim.rename(columns={'Borough':'PU_Borough','Zone':'PU_Zone'},inplace=True)

    DOLocation_dim = pd.merge(df['DOLocationID'], args[0], how='left', left_on='DOLocationID', right_on='LocationID')
    DOLocation_dim = DOLocation_dim[['DOLocationID','Borough','Zone']].reset_index(drop=True)
    DOLocation_dim.rename(columns={'Borough':'DO_Borough','Zone':'DO_Zone'},inplace=True)


    location_dim = pd.concat([PULocation_dim, DOLocation_dim], axis=1)
    location_dim['Location_ID'] = location_dim.index
    location_dim = location_dim[['Location_ID', 'PULocationID','PU_Borough','PU_Zone','DOLocationID','DO_Borough','DO_Zone']].reset_index(drop=True) 
    
    trip_dim = df[['trip_miles','trip_time']].reset_index(drop=True)
    trip_dim['trip_id'] = trip_dim.index
    trip_dim = trip_dim[['trip_id','trip_miles','trip_time']].reset_index(drop=True)

    trip_dim['trip_time_converted'] = trip_dim['trip_time'].apply(lambda seconds: '{:02d}:{:02d}:{:02d}'.format(seconds // 3600, (seconds % 3600) // 60, seconds % 60))

    trip_dim = trip_dim[['trip_id', 'trip_miles','trip_time_converted']].reset_index(drop=True)

    fact_table = df.merge(datetime_dim, left_on='ID',right_on='datetime_id').merge(service_license_dim, left_on='ID', right_on='service_id').merge(location_dim, left_on='ID', right_on='Location_ID').merge(trip_dim, left_on='ID', right_on='trip_id')[['ID','datetime_id','service_id', 'Location_ID','trip_id','base_passenger_fare','tolls',
                                                                                                                                                                                                                                                      'bcf','sales_tax','congestion_surcharge','airport_fee','tips',
                                                                                                                                                                                                                                                      'driver_pay','shared_request_flag','shared_match_flag',
                                                                                                                                                                                                                                                      'access_a_ride_flag','wav_request_flag','wav_match_flag']]                       
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "service_license_dim":service_license_dim.to_dict(orient="dict"),
    "location_dim":location_dim.to_dict(orient="dict"),
    "trip_dim":trip_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
