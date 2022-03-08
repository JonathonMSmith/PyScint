# -*- coding: utf-8 -*-
"""
Loads data from the Chain network

CHAIN has 25 GNSS Ionospheric Scintillation and TEC Monitors (GISTM) located
throughout the Canadian Arctic. Each GISTM operates as a dual-band
Global Navigation Satellite System (GNSS) receiver as well as a GNSS
Ionospheric Scintillation and TECM Monitor (GISTM). GNSS data is collected at
1Hz, while GISTM data is collected at 50Hz.

Properties
----------
platform
    chain
name
    gps
inst_id
    'arc': 'arctic_bay', 'arv': 'arviat', 'cbb': 'cambridge_bay',
    'chu': 'churchill', 'cor': 'coral_harbour', 'edm': 'edmonton',
    'eur': 'eureka', 'mcm': 'fort_mcmurray', 'fsi': 'fort_simpson',
    'fsm': 'fort_smith', 'gil': 'gillam', 'gjo': 'gjoa_haven',
    'gri': 'grise_fiord', 'hal': 'hall_beach', 'iqa': 'iqaluit',
    'kug': 'kugliktuk', 'pon': 'pond_inlet', 'qik': 'qikiqtarjuaq',
    'rab': 'rabbit_lake', 'ran': 'rankin_inlet', 'rep': 'repulse_bay',
    'res': 'resolute', 'sac': 'sachs_harbour', 'san': 'sanikiluaq',
    'tal': 'taloyoak'
tag
    'daily', 'highrate', 'hourly', 'local', 'nvd', 'raw', 'sbf'

Note
----
- Optional section, remove if no notes

Warnings
--------
- Optional section, remove if no warnings
- Two blank lines needed afterward for proper formatting


Examples
--------
::

    Example code can go here


Authors
-------
Jonathon M. Smith

"""
import georinex as gr
import warnings
import datetime
import os
import sys
import ftplib
import pysat
from pysat import logger
from PyScint.instruments.methods import chain as mm_chain


platform = 'chain'
name = 'gps'
tags = {'daily': 'Daily Files', 'highrate': 'High rate data',
        'hourly': 'Hourly batched data files', 'local': '', 'nvd': '',
        'raw': '', 'sbf': ''}
inst_ids = {'arc': 'arctic_bay', 'arv': 'arviat', 'cbb': 'cambridge_bay',
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
             'L1': ('full_cycles', 'carrier_phase_1575.42MHz'),
             'L2': ('full_cycles', 'carrier_phase_1227.60MHz'),
             'L5': ('full_cycles', 'carrier_phase_1176.45MHz'),
             'S1': ('SNR', 'signal_strength'),
             'S2': ('SNR', 'signal_strength'),
             'S5': ('SNR', 'signal_strength')}

pandas_format = False

# ----------------------------------------------------------------------------
# Instrument methods


def init(self):
    """Initializes the Instrument object with instrument specific values.

    Runs once upon instantiation.

    Parameters
    -----------
    inst : pysat.Instrument
        Instrument class object

    """

    logger.info(mm_chain.ackn_str)
    self.acknowledgements = mm_chain.ackn_str
    self.references = mm_chain.refs['chain']

    return


def clean(self):
    """Routine to return CHAIN data cleaned to the specified level

     Parameters
    ----------
    self : pysat.Instrument
        Instrument class object, whose attribute clean_level is used to return
        the desired level of data selectivity.

    Note
    ----
    'clean' - Not specified
    'dusty' - Not specified
    'dirty' - Not specified
    'none' No cleaning applied, routine not called
    """
    warnings.warn('No cleaning routines available for CHAIN GPS')

    return


# ----------------------------------------------------------------------------
# Instrument functions


def list_files(tag=None, inst_id=None, data_path=None, format_str=None):
    """Return a Pandas Series of every file for chosen receiver data.
    Parameters
    ----------
    tag : (string or NoneType)
        Denotes type of file to load.
        (default=None)
    inst_id : (string or NoneType)
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
    if format_str is None:
        # user did not supply an alternative format template string
        format_str = '???c{day:03d}{hour:1d}.{year:02d}?.Z'
    # we use a pysat provided function to grab list of files from the
    # local file system that match the format defined above
    file_list = pysat.Files.from_os(data_path=data_path, format_str=format_str,
                                    two_digit_year_break=90)

    return file_list


def load(fnames, tag=None, inst_id=None):
    """Load CHAIN GPS Files
    Parameters
    ----------
    fnames : (list or array-like)
        series of filenames to be loaded
    tag : (string or NoneType)
        Denotes type of file to load.
        (default=None)
    inst_id : (string or NoneType)
        Specifies the satellite ID for a constellation.  Not used.
        (default=None)
    """

    if not fnames:
        print('fnames are go')
        warnings.warn('You no got no fname, you no get no data')
        return pysat.DataFrame(None), pysat.Meta(None)
    elif len(fnames) == 1:
        print('fnames are greater than 1')
        meta = pysat.Meta()
        signal_meta = pysat.Meta()

        # load the rinex
        data = gr.load(fnames[0])
        # get the metadata from the xarray.Dataset
        xr_attrs = data.attrs
        # format the metadata
#        for at in xr_attrs:
#            print(type(at))
#            meta[at] = {'units': '', 'long_name': at}
#        keys = data.data_vars.keys()
#        for key in keys:
#            # find the xarray equivalents of units and long_name
#            print(key)
#            signal_meta[key] = {meta.labels.units: var_units[key][0],
#                                meta.labels.name: var_units[key][1]}
#        # format the data
#        meta['signals'] = signal_meta
#        output = data.variables
        return data, meta
    else:
        raise ValueError('Only one filename currently supported')
    print('no idea wahts goin on')


def download(date_array, tag, inst_id, data_path=None, user=None, password=None,
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

    if tag not in tags:
        raise ValueError('Uknown CHAIN tag')
    elif (user is None) or (password is None):
        raise ValueError('CHAIN user account information must be provided.')

    top_dir = os.path.join(data_path)

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
                if inst_id:
                    print(inst_id)
                    if file[0:3] != inst_id:
                        continue
                save_dir = os.path.join(top_dir)
                print(save_dir)
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
