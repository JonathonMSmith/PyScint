import numpy as np
import pandas as pd


def simple_s4(snr: list = None, bin_size: int = 600, step_size: int = 600):
    """Compute S4 from the signal to noise ratio
       using the equation: s_4 = var_intensity / intensity
       intensity = 10 ** (snr / 10)
       Parameters
       ==========
       snr : arry-like
           time series of signal to noise ratio from NMEA message
       bin_size : int
           number of sample points to consititute a sample
       step_size : int
           number of sample points to step for each sample. For example,
               a step size of 1 would make a new sample of size bin_size every
               1 data point
    """
    intensity = 10 ** (snr / 10)
    var_intensity = np.var(intensity)
