"""
Loads data from the Chain network

CHAIN has 25 GNSS Ionospheric Scintillation and TEC Monitors (GISTM) located
throughout the Canadian Arctic. Each GISTM operates as a dual-band
Global Navigation Satellite System (GNSS) receiver as well as a GNSS
Ionospheric Scintillation and TECM Monitor (GISTM). GNSS data is collected at
1Hz, while GISTM data is collected at 50Hz.

"""
import georinex as gr
import logging
import datetime
import os
import sys
import pysat
logger = logging.getLogger(__name__)

platform = 'chain'
name = 'gps'
tags = ['daily', 'highrate', 'hourly', 'local', 'nvd', 'raw', 'sbf']
sat_ids = {'arc': 'arctic_bay', 'arv': 'arviat', 'cbb': 'cambridge_bay',
           'chu': 'churchill', 'cor': 'coral_harbour', 'edm': 'edmonton',
           'eur': 'eureka', 'mcm': 'fort_mcmurray', 'fsi': 'fort_simpson',
           'fsm': 'fort_smith', 'gil': 'gillam', 'gjo': 'gjoa_haven',
           'gri': 'grise_fiord', 'hal': 'hall_beach', 'iqa': 'iqaluit',
           'kug': 'kugliktuk', 'pon': 'pond_inlet', 'qik': 'qikiqtarjuaq',
           'rab': 'rabbit_lake', 'ran': 'rankin_inlet', 'rep': 'repulse_bay',
           'res': 'resolute', 'sac': 'sachs_harbour', 'san': 'sanikiluaq',
           'tal': 'taloyoak'}

# signal strength SNR units are receiver dependent. See RINEX-211.pdf
var_units = {'C1': ('meters', 'pseudorange'), 'P1': ('meters', 'pseudorange'),
             'C2': ('meters', 'pseudorange'), 'P2': ('meters', 'pseudorange'),
             'C5': ('meters', 'pseudorange'),
             'L1': ('full_cycles', 'carrier_phase'),
             'L2': ('full_cycles', 'carrier_phase'),
             'L5': ('full_cycles', 'carrier_phase'),
             'S1': ('SNR', 'signal_strength'),
             'S2': ('SNR', 'signal_strength'),
             'S5': ('SNR', 'signal_strength')}


# currently using sat_id convention, although this is not a satellite.
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
    # update this time estimate based on actual results
    print('{:s}~1s per 100K files'.format(estr))
    sys.stdout.flush()

    return file_list


def load(fnames, tag=None, sat_id=None):
    """Load CHAIN GPS Files
    Parameters
    ----------
    fnames : (list or array-like)
        series of filenames to be loaded
    tag : (string or NoneType)
        Denotes type of file to load.
        (default=None)
    sat_id : (string or NoneType)
        Specifies the satellite ID for a constellation.  Not used.
        (default=None)
    """

    if not fnames:
        return pysat.DataFrame(None), pysat.Meta(None)
    elif len(fnames) > 1:

        meta = pysat.Meta()
        signal_meta = pysat.Meta()

        # load the rinex
        data = gr.load(fnames[0])
        # get the metadata from the xarray.Dataset
        xr_attrs = data.attrs
        # format the metadata
        for at in xr_attrs:
            meta[at] = {'units': '', 'long_name': at}
        keys = data.data_vars.keys()
        for key in keys:
            # find the xarray equivalents of units and long_name
            signal_meta[key] = {'units': data.variables[key].units,
                                'long_name': data.variables[key].long_name}
        # format the data
        output = data.variables
        return output, meta
    else:
        raise ValueError('Only one filename currently supported')


def download(date_array, tag, data_path=None, user=None, password=None,
             compression_type='o'):
    """Download Chain Data
    For tags
    Path format for daily, highrate, hourly, local:
    ftp://chain.physics.unb.ca/gps/data/tag/YYYY/DDD/YYo/
    nvd, raw, sbf:
    ftp://chain.physics.unb.ca/gps/data/nvd/STN/YYYY/MM/

    Currently only daily is confirmed to work

    Parameters
    ----------
    date_array : list of datetime.datetime

    tag : string
        daily, highrate, hourly, local, nvd, raw, sbf

    compression_type : string
        o - observation .Z UNIX compression
        d - Hatanaka AND UNIX compression
    """
    import ftplib

    if tag not in tags:
        raise ValueError('Uknown CHAIN tag')
    elif (user is None) or (password is None):
        raise ValueError('CHAIN user account information must be provided.')

    top_dir = os.path.join(data_path, 'chain')

    for date in date_array:
        logger.info('Downloading COSMIC data for ' + date.strftime('%D'))
        sys.stdout.flush()
        yr = date.strftime('%Y')
        doy = date.strftime('%j')

        # try download
        try:
            # ftplib uses a hostname not a url, so the 'ftp://' is not here
            # connect to ftp server and change to desired directory
            hostname = ''.join(('chain.physics.unb.ca'))
            ftp = ftplib.FTP(hostname)
            ftp.login(user, password)
            ftp_dir = ''.join(('/gps/data/', tag, '/', yr, '/', doy, '/',
                               yr[-2:], compression_type, '/'))
            ftp.cwd(ftp_dir)

            # setup list of station files to iterate through
            files = []
            ftp.retrlines('LIST', files.append)
            files = [file.split(None)[-1] for file in files]
            # iterate through files and download each one
            for file in files:
                save_dir = os.path.join(top_dir, ftp_dir[1::])
                # make directory if it doesn't already exist
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)

                save_file = os.path.join(save_dir, file)
                with open(save_file, 'wb') as f:
                    print('Downloading: ' + file + ', and saving to ' +
                          save_file)
                    ftp.retrbinary("RETR " + file, f.write)

        except ftplib.error_perm as err:
            # pass error message through and log it
            estr = ''.join((str(err)))
            print(estr)
            logger.info(estr)

    ftp.close()
    return
