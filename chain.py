"""
Loads data from the Chain network

CHAIN has 25 GNSS Ionospheric Scintillation and TEC Monitors (GISTM) located
throughout the Canadian Arctic. Each GISTM operates as a dual-band
Global Navigation Satellite System (GNSS) receiver as well as a GNSS Ionospheric
Scintillation and TECM Monitor (GISTM). GNSS data is collected at 1Hz, while
GISTM data is collected at 50Hz.

"""
import georinex
import logging
import datetime
import os
import sys
logger = logging.getLogger(__name__)

platform = 'chain'
name = 'gps'
tags = ['daily', 'highrate', 'hourly', 'local', 'nvd', 'raw', 'sbf']

#currently using sat_id convention, although this is not a satellite.
def list_files(tag=None, sat_id=None, data_path=None, format_str=None):
    """Return a Pandas Series of every file for chosen receiver data.
    Parameters
    ----------
    tag : (string or NoneType)
        Denotes type of file to load.
        (default=None)
    sat_id : (string or NoneType)
        Specifies the satellite ID for a constellation.  Not used.
        (default=None)
    data_path : (string or NoneType)
        Path to data directory.  If None is specified, the value previously
        set in Instrument.files.data_path is used.  (default=None)
    format_str : (NoneType)
        User specified file format not supported here. (default=None)
    Returns
    -------
    pysat.Files.from_os : (pysat._files.Files)
        A class containing the verified available files
    """
    estr = 'Building a list of CHAIN files, which can possibly take time. '
    #update this time estimate based on actual results
    print('{:s}~1s per 100K files'.format(estr))
    sys.stdout.flush()

    return file_list

def load():
    return


def download(date_array, tag, data_path=None, user=None, password=None):
    """Download Chain Data
    For tags
    Path format for daily, highrate, hourly, local:
    ftp://chain.physics.unb.ca/gps/data/tag/YYYY/DDD/YYo/
    nvd, raw, sbf:
    ftp://chain.physics.unb.ca/gps/data/nvd/STN/YYYY/MM/

    Currently only daily is confirmed to work

    Parameters
    ==========
    date_array : datetime.datetime

    tag : string
        daily, highrate, hourly, local, nvd, raw, sbf
    """
    import ftplib

    if tag not in tags:
        raise ValueError('Uknown CHAIN tag')

    if (user is None) or (password is None):
        raise ValueError('CHAIN user account information must be provided.')

    top_dir = os.path.join(data_path, 'chain')
    print(top_dir)

    for date in date_array:
        logger.info('Downloading COSMIC data for ' + date.strftime('%D'))
        sys.stdout.flush()
        yr = date.strftime('%Y')
        doy = date.strftime('%j')
        yrdoystr = ''.join((yr, '.', doy))
        #try download
        try:
            #ftplib uses a hostname not a url, so the 'ftp://' is not here
            hostname = ''.join(('chain.physics.unb.ca'))
            ftp = ftplib.FTP(hostname)
            ftp.login(user, password)
            ftp_dir = ''.join(('/gps/data/', tag, '/', yr, '/', doy, '/',
                         #'/{year:04d}/{doy:03d}/'.format(year=yr, doy=doy),
                               yr[-2:], 'o/'))
            print(ftp_dir)
            ftp.cwd(ftp_dir)

            files = []
            ftp.retrlines('LIST', files.append)
            files = [file.split(None)[-1] for file in files]

            for file in files:
                save_dir = os.path.join(top_dir, ftp_dir)
                print(save_dir)
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                save_file = os.path.join(save_dir, file)
                print(save_file)
                with open(save_file, 'wb') as f:
                    print('Downloading: ' + file)
                    ftp.retrbinary("RETR " + file, f.write)
        except ftplib.error_perm as err:
             estr = ''.join((str(err)))
             print(estr)
             logger.info(estr)

    ftp.close()
    return
