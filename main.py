
from prob_model import  new_probability_model
import function_store2 as fs
import time
#from scipy.stats import norm
#from statistics import NormalDist as norm
import math
import numpy as np
import pickle
import pandas as pd

spd = pickle.load(open('sample_data.pkl','rb'))
timestamp = list(spd.keys())[6]
opt_ltps = spd[timestamp]['opt_ltps']
opt_ois = spd[timestamp]['opt_oi']
mean = spd[timestamp]['mean']
sd = spd[timestamp]['sd']

print("Running Cython")
cythonp = new_probability_model(opt_ltps, opt_ois,mean,sd)
print("Running Python")
pythonp = fs.probability_model(opt_ltps,opt_ois,mean,sd)
