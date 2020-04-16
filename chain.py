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

    for date in date_array:
        logger.info('Downloading COSMIC data for ' + date.strftime('%D'))
        sys.stdout.flush()
        yr = date.strftime(%Y)
        doy = date.strftime(%j)
        yrdoystr = ''.join(yr, '.', doy)
        #try download
        try:
            dwnld = ''.join('ftp://chain.physics.unb.ca/gps/data/', tag,
                            '/{year:04d}/{doy:03d}/'.format(year=yr, doy=doy),
                            yr[-2:], '/')
            top_dir = os.path.join(data_path, 'chain')
            ftp = ftplib.FTP(dwnld, user, password)
            ftp.login()                                                       
    return
