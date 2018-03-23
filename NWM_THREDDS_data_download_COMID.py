from pydap.client import open_url
import datetime
import csv


start_time = datetime.datetime.now()


def openNWM(forecast_range, download_date, z_time, configuration, forecast_time):
    # The method to open the OPeNDAP url
    ## EXAMPLE OF VALID INPUTS:
    # forecast_range = 'analysis_assim'
    # download_date in 'YYYYMMDD' format = '20180306' (last 42 days)
    # z_time = '00','01',..,'23'.
    # configuration = 'channel_rt'
    # forecast_time = #short_range:'001','002',..,'018'; #medium_range:'003','006',..,'240';
    # variables = 'velocity' , 'streamflow'
    # IMPORTANT: Number of COMIDs: 2.7 million (2716896 and 2729076 for short/medium or analysis_assim, respectively)

    # analysis_assim
    # 'http://tds.renci.org:8080/thredds/dodsC/nwm/analysis_assim/nwm.20180322.t00z.analysis_assim.channel_rt.tm02.conus.' \
    # 'nc.ascii?feature_id[0:1:2729076],streamflow[0:1:2729076],velocity[0:1:2729076]''
    if forecast_range == 'analysis_assim':
        url = 'http://tds.renci.org:8080/thredds/dodsC/nwm/{f_range}/nwm.{date}.t{ztime}z.{f_range}.{config}.tm02.conus.nc'.format(
            f_range=forecast_range,
            date=download_date,
            ztime=z_time,
            config=configuration)
    elif forecast_range == 'medium_range' or forecast_range == 'short_range':
        url = 'http://tds.renci.org:8080/thredds/dodsC/nwm/{f_range}/{date}/nwm.t{ztime}z.{f_range}.{config}.f{ftime}.conus.nc'.format(
            f_range=forecast_range,
            date=download_date,
            ztime=z_time,
            config=configuration,
            ftime=forecast_time)
    else:
        print 'The input is not valid. Please enter valid forecast range'

    # Opens url
    dataset = open_url(url)

    feature_id = dataset['feature_id']
    velocity = dataset['velocity']
    streamflow = dataset['streamflow']

    return feature_id, velocity, streamflow


def date_time_values(forecast_range):
    # Define the forecast method to create the lists of dates, ztime and ftime for short_range/medioum_range
    # EXAMPLE OF VALID INPUTS:
    # forecast_range = 'short_range', 'medium_range'

    # Create the list of last 42 days
    dates = []
    ztime = []
    ftime = []
    now = datetime.datetime.now()
    for j in range(42):
        dates.append(now - datetime.timedelta(j))
        dates[j] = dates[j].strftime('%Y%m%d')

    # analysis_assim
    if str(forecast_range) == 'analysis_assim':
        # Create the list of hour 00,01,..,23
        for j in range(24):
            ztime.append(j)
            ztime[j] = str(ztime[j])
            if int(ztime[j]) < 10:
                ztime[j] = '0' + ztime[j]

        # To make sure that the in the main program ftime loop runs one time for analysis_assim, just put a value:
        ftime = ['0']

    # short_range
    elif str(forecast_range) == 'short_range':
        # Create the list of hour 00,01,..,23
        for j in range(24):
            ztime.append(j)
            ztime[j] = str(ztime[j])
            if int(ztime[j]) < 10:
                ztime[j] = '0' + ztime[j]

        # Create the list of forecast time for short_range: 01,02,..,18
        for j in range(1, 19):
            ftime.append(j)
            ftime[j-1] = str(ftime[j-1])
            if int(ftime[j-1]) < 10:
                ftime[j-1] = '00' + ftime[j-1]
            elif int(ftime[j-1]) < 19:
                ftime[j-1] = '0' + ftime[j-1]

    # medium_range
    elif str(forecast_range) == 'medium_range':
        # Create the list of hour 00,06,..,18
        for j in range(0, 24, 6):
            ztime.append(j)
            ztime[j / 6] = str(ztime[j / 6])
            if int(ztime[j / 6]) < 10:
                ztime[j / 6] = '0' + ztime[j / 6]

        # Create the list of forecast time for medium_range forecast for 3 hours interval up to 10 days
        for j in range(3, 243, 3):
            ftime.append(j)
            ftime[j / 3 - 1] = str(ftime[j / 3 - 1])
            if int(ftime[j / 3 - 1]) < 10:
                ftime[j / 3 - 1] = '00' + ftime[j / 3 - 1]
            elif int(ftime[j / 3 - 1]) < 100:
                ftime[j / 3 - 1] = '0' + ftime[j / 3 - 1]
    else:
        print 'The input is not valid. Please enter valid forecast range'

    return dates, ztime, ftime

##################################################################################################
# ***************************************** Main Program *****************************************
##################################################################################################

# Identifies the positions of the desired COMIDs in the matrix of 2.7 million COMIDs

# Here choose forecast range: 'short_range', 'medium_range' or 'analysis_assim'
forecast_range = 'medium_range'
dates, ztime, ftime = date_time_values(forecast_range)

# USGS: 03612600  => COMID : '1841415'
# USGS: 01665500  => COMID : '8468791'
# USGS: 11447850   => COMID : '15059771'

COMID_list = ['15059771', '8468791', '1841415']
COMID_position = []
feature_id = []
feature_id = openNWM(forecast_range, dates[20], '00', 'channel_rt', '003')[0]
feature_id_arr = feature_id.__array__()

for item in range(len(COMID_list)):
    for i in range(len(feature_id_arr)):
        if COMID_list[item] == str(feature_id_arr[i]):
            COMID_position.append(i)
            break

# Download and store the available data for COMIDs

for item in range(len(COMID_list)):
    filename = str(COMID_list[item]) + '_' + forecast_range + '.csv'
    with open(filename, 'a') as csvfile:
        NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
        NWM_writer.writerow(['#', 'forecast_range:', forecast_range, 'COMID:', COMID_list[item]])
        NWM_writer.writerow(['date', 'ztime', 'ftime', 'velocity(m/s)', 'streamflow(m^3/s)'])

for d_count in range(len(dates)-5, 36, -1):
    for zt_count in range(len(ztime)):
        for ft_count in range(len(ftime)):
                feature_id, velocity, streamflow = openNWM(forecast_range, dates[d_count], ztime[zt_count],
                    'channel_rt', ftime[ft_count])
                scale_factor = 0.01

                # with open('testNWM', 'w') as f:
                for pos, comid in enumerate(COMID_list):
                    filename = str(comid) + '_' + forecast_range + '.csv'
                    velocity_value = velocity[COMID_position[pos]].data[0] * scale_factor
                    streamflow_value = streamflow[COMID_position[pos]].data[0] * scale_factor
                    with open(filename, 'a') as csvfile:
                        NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
                        NWM_writer.writerow([dates[d_count], ztime[zt_count], ftime[ft_count],
                                             velocity_value,
                                             streamflow_value])

elapsed_time = datetime.datetime.now() - start_time
print 'Elapsed time:', elapsed_time
