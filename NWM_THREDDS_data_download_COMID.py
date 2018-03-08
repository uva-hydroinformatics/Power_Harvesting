from pydap.client import open_url
import datetime
import csv


def openNWM(forecast_range, download_date, z_time, configuration, forecast_time, variable1, variable2,
            start_comid, end_comid):
    # Defines the method to open the OPeNDAP url
    ## EXAMPLE OF VALID INPUTS:
    # forecast_range = 'short_range', 'medium_range'
    # download_date in 'YYYYMMDD' format = '20180306' (last 42 days)
    # z_time = '00','01',..,'23'.
    # configuration = 'channel_rt'
    # forecast_time = #short_range:'001','002',..,'018'; #medium_range:'003','006',..,'240';
    # variable = 'velocity' , 'streamflow'
    # 2729076
    # Creating the url
    url = 'http://thredds.hydroshare.org/thredds/dodsC/nwm/{f_range}/{date}/nwm.t{ztime}z.{f_range}.' \
          '{config}.f{ftime}.conus.nc?{var1}[{st_comid}:1:{e_comid}],reference_time[0:1:0],time[0:1:0],' \
          'feature_id[{st_comid}:1:{e_comid}],{var2}[{st_comid}:1:{e_comid}]'.format(
        f_range=forecast_range,
        date=download_date,
        ztime=z_time,
        config=configuration,
        ftime=forecast_time,
        var1=variable1,
        var2=variable2,
        st_comid=start_comid,
        e_comid=end_comid)

    # Opens url
    dataset = open_url(url)

    feature_id = dataset['feature_id']
    velocity = dataset['velocity']
    streamflow =dataset['streamflow']

    return feature_id.__array__(), velocity.__array__(), streamflow.__array__()


def decide_range(forecast_range):
    # Define the forecast method to create the lists of dates, ztime and ftime for short_range/medioum_range
    # EXAMPLE OF VALID INPUTS:
    # forecast_range = 'short_range', 'medium_range'

    # Create the list of last 42 days
    dates = []
    now = datetime.datetime.now()
    for j in range(42):
        dates.append(now - datetime.timedelta(j))
        dates[j] = dates[j].strftime('%Y%m%d')

    # short_range
    if str(forecast_range) == 'short_range':
        # Create the list of hour 00,01,..,23
        ztime = []
        for j in range(24):
            ztime.append(j)
            ztime[j] = str(ztime[j])
            if int(ztime[j]) < 10:
                ztime[j] = '0' + ztime[j]

        # Create the list of forecast time for short_range
        ftime = []
        for j in range(1,19):
            ftime.append(j)
            ftime[j] = str(ftime[j])
            if int(ftime[j]) < 10:
                ftime[j] = '00' + ftime[j]
            elif int(ftime[j]) < 19:
                ftime[j] = '0' + ftime[j]
    ####medium_range
    elif str(forecast_range) == 'medium_range':
        # Create the list of hour 00,06,..,18
        ztime = []
        for j in range(0, 24, 6):
            ztime.append(j)
            ztime[j / 6] = str(ztime[j / 6])
            if int(ztime[j / 6]) < 10:
                ztime[j / 6] = '0' + ztime[j / 6]

        # Create the list of forecast time for medium_range forecast for 3 hours interval up to 10 days
        ftime = []
        for j in range(3, 243, 3):
            ftime.append(j)
            ftime[j / 3 - 1] = str(ftime[j / 3 - 1])
            if int(ftime[j / 3 - 1]) < 10:
                ftime[j / 3 - 1] = '00' + ftime[j / 3 - 1]
            elif int(ftime[j / 3 - 1]) < 100:
                ftime[j / 3 - 1] = '0' + ftime[j / 3 - 1]

    return dates, ztime, ftime


##################################################################################################
# ***************************************** Main Program *****************************************
##################################################################################################

# Identifies the positions of the desired COMIDs in the matrix of 2.7 million COMIDs

# Here choose forecast range: short or medium?
# forecast_range= 'short_range'
forecast_range = 'medium_range'
dates, ztime, ftime = decide_range(forecast_range)

COMID = ['867', '849']
COMID_position = []
feature_id = openNWM('medium_range', dates[2], '00', 'channel_rt', '003', 'velocity', 'streamflow', '0', '25')[0]
for item in range(len(COMID)):
    for i in range(len(feature_id.__array__())):
        if COMID[item] == str(feature_id.__array__()[i]):
            COMID_position.append(i)


# Download and store available data for COMIDs
Error_count = 0

for item in range(len(COMID)):
    filename = str(COMID[item]) + '.csv'
    with open(filename, 'a') as csvfile:
        NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
        NWM_writer.writerow(['#', forecast_range, COMID[item]])
        NWM_writer.writerow(['date', 'ztime', 'ftime', 'velocity(m/s)', 'streamflow(m^3/s)'])

for d_count in range(len(dates)-2, 35, -1):
    for zt_count in range(len(ztime)):
        for ft_count in range(len(ftime)):

            try:
                feature_id, velocity, streamflow = openNWM(forecast_range, dates[d_count], ztime[zt_count], 'channel_rt',
                                               ftime[ft_count], 'velocity', 'streamflow', '0', '25')
                scale_factor = 0.01
                velocity = velocity * scale_factor
                streamflow = streamflow *scale_factor

                # with open('testNWM', 'w') as f:
                for item in range(len(COMID)):
                    filename = str(COMID[item]) + '.csv'
                    with open(filename, 'a') as csvfile:
                        NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
                        NWM_writer.writerow([dates[d_count], ztime[zt_count], ftime[ft_count],
                                             float(velocity.__array__()[COMID_position[item]]),
                                             float(streamflow.__array__()[COMID_position[item]])])

            except ValueError as e:
                Error_count = Error_count + 1  # counts missing data
