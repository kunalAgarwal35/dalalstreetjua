from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules = [
        Extension(r'prob_model', [r'prob_model.pyx'])]

setup(name = 'prob_model', ext_modules = cythonize(ext_modules, annotate=True),)
