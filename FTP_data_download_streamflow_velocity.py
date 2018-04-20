import os
from ftplib import FTP
from datetime import datetime, timedelta
import csv

from pydap.client import open_url
from pydap.exceptions import ServerError
import xarray as xr
import numpy as np
import time

# define the NWM main FTP URL.
ftp = FTP("ftpprd.ncep.noaa.gov")
ftp.login()

COMID_list = ["1841415", "8468791"]
# create a local folder to store the downloaded data.
destination = "./NWM/"
if not os.path.exists(destination):
    os.makedirs(destination)

# data can be downloaded only for the current day and one day older as we are using the official
# website for the NWM.
# "timedelta(days=0)": download the current date
# "timedelta(days=1)": download one day older from the current date


streamflow = np.zeros((2, len(COMID_list), 24))
velocity = np.zeros((2, len(COMID_list), 24))
i = 0
j = 0

for i in range(2):
    target_date = str(datetime.now().date() - timedelta(days=i)).replace("-", "")
    if not os.path.exists(destination + "/" + target_date):
        os.makedirs(destination + "/" + target_date)

    # get the whole list of the available data for the target day
    nwm_data = "/pub/data/nccf/com/nwm/prod/nwm." + target_date + "/"
    ftp.cwd(nwm_data)

    # by default, all the data folder will be downloaded. In case you would like to download a specific
    # folder, change the following line to "target_data_folder = ["NAME OF FOLDER"]".
    # The currently available folders are ['analysis_assim', 'forcing_analysis_assim',
    # 'forcing_medium_range', 'forcing_short_range', 'long_range_mem1', 'long_range_mem2',
    # 'long_range_mem3', 'long_range_mem4', 'medium_range', 'short_range', 'usgs_timeslices']
    # target_data_folder = ftp.nlst()
    # target_data_folder = ['analysis_assim', 'short_range', 'medium_range']
    target_data_folder = ['analysis_assim']
    # time_minus can be "tm00", "tm01" or "tm02"; it is used for analysis and assimilation:
    # It is the number of hours before the current cycle time that the data is valid, for now it just works for tm00,
    # later I should do minor adjustments to include tm01 and tm02
    time_minus = "tm00"
    print target_data_folder

    for comid in COMID_list:
        filename = str(comid) + '_' + target_date + '.csv'
        with open(filename, 'a') as csvfile:
            NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
            NWM_writer.writerow(['#', 'target_date:', target_date, 'COMID:', comid])
            NWM_writer.writerow(['date', 'ztime(utc)', 'streamflow(m^3/s)', 'velocity(m/s)'])

    # download the available data for the target date and data folder/s
    for data_type in target_data_folder:
        data_type_path = "/pub/data/nccf/com/nwm/prod/nwm." + target_date + "/" + data_type + "/"
        dest_data_path = destination + "/" + target_date + "/" + data_type
        if not os.path.exists(dest_data_path):
            os.makedirs(dest_data_path)
        ftp.cwd(data_type_path)
        filelist = ftp.nlst()
        print filelist

        # download the available files in the target folder/s
        for file in filelist:
            file_info = file.split(".")
            if file_info[3] == "channel_rt" and file_info[4] == "tm00":
                ftp.retrbinary("RETR " + file, open(os.path.join(dest_data_path, file), "wb").write)
                ds = xr.open_dataset(os.path.join(dest_data_path, file))
                df = ds.to_dataframe()

                for pos, comid in enumerate(COMID_list):
                    streamflow[i][pos][j] = float(str(df.ix[int(comid), ['streamflow']]).split(" ")[-1])
                    velocity[i][pos][j] = float(str(df.ix[int(comid), ['velocity']]).split(" ")[-1])

                    filename = str(comid) + '_' + target_date + '.csv'
                    with open(filename, 'a') as csvfile:
                        NWM_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
                        NWM_writer.writerow([target_date, int(file_info[1][1:3]), streamflow[i][pos][j], velocity[i][pos][j]])
                        # int(file_info[1][1:3]) must be = j
                print file + " downloaded"
                j += 1

    j = 0




    print "Done downloading the NWM data for the target date:" + str(target_date) + "!"