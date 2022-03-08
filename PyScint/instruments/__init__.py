__all__ = ['chain_gps']

for inst in __all__:
    exec("from PyScint.instruments import {x}".format(x=inst))
