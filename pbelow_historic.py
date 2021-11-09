import pickle
import datetime
aux_files = 'optionadjustbacktest/'
insname = 'NIFTY'
fname = 'pbelow_'+insname+'_'
def load_pbelow():
    pbelow = pickle.load(open(aux_files+fname,'rb'))
    pbelow_new = {}
    for key in pbelow.keys():
        pbelow_new[key.replace(microsecond = 0)] = pbelow[key]
    return pbelow_new


pbelow = load_pbelow()

def find_pbelow(timestamp):
    key = timestamp.replace(microsecond = 0)
    if key in pbelow.keys():
        return pbelow[key]
    else:
        return {}



