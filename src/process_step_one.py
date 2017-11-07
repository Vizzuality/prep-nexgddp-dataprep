##########
# IMPORT #
##########

import os
import sys
import time
import logging

#import boto3
#import botocore

import gdal
from osgeo import osr
import netCDF4

import numpy as np
import xarray as xr
import itertools
import matplotlib.pyplot as plt
import numba as nb
import pandas as pd
from urllib import request
import dask

from multiprocessing import Pool
import multiprocessing_logging

###########
# LOGGING #
###########

logging.basicConfig(
     level = 'DEBUG',
     format = '%(asctime)s - %(levelname)s - %(message)s'
)
multiprocessing_logging.install_mp_handler()

###############
# GLOBAL VARS #
###############
# NEXGDDP data should be found here
data_url = 'http://mymachine:8080'
# data_url = 'http://nasanex.s3.amazonaws.com'
#
# Do not change unless you have a good reason
file_prefix = 'data' # < data is exposed in dev mode
download_prefix = 'downloads'
# < but downloads is not - file downloads not already processed
# are lost on container exit
#
max_download_attempts = 5
#
all_scenarios = ['historical', 'rcp45', 'rcp85']
all_models = ['ACCESS1-0', 'BNU-ESM', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'inmcm4']
all_models = ['ACCESS1-0', 'BNU-ESM']
# all_models = ['GFDL-CM3']
# all_vars = ['pr', 'tasmax', 'tasmin']
all_vars = ['tasmax', 'tasmin']
all_years = {
     'historical': list(range(1950, 1981)),
     'rcp45': list(range(2006, 2101)),
     'rcp85': list(range(2006, 2101))
}

# Output corners and size
xmin, ymin, xmax, ymax = [-180, -90, 180, 90]
nrows, ncols = 720, 1440
xres = (xmax - xmin) / float(ncols)
yres = (ymax - ymin) / float(nrows)
# ^ Used for definining a geotransform for the output data
geotransform = (xmin, xres, 0, ymax, 0, -yres)

#############
# FUNCTIONS #
#############

def download_file(url):
     attempts = 0
     success = False
     while attempts < max_download_attempts and not success:
          try:
               logging.info(f"Downloading file at url: {url}")
               time.sleep(2 ** attempts)
               filename = download_prefix + '/' + str(url.split('/')[-1])
               u = request.urlopen(url)
               f = open(filename, 'wb')
               f.write(u.read())
               f.close()
               success = True
               break
          except:
               logging.error("Problem downloading the file. Retrying.")
               attempts += 1
     return success

def get_url(variable, scenario, model, year, prefix = data_url):
     # /NEX-GDDP/BCSD/rcp85/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_rcp85_r1i1p1_inmcm4_2096.nc
     filename = f"{prefix}/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{str(year)}.nc"
     return filename

def get_context(**kwargs):
     variables = [kwargs.get('variable')] if kwargs.get('variable') else all_vars
     scenarios = [kwargs.get('scenario')] if kwargs.get('scenario') else all_scenarios
     models = [kwargs.get('model')] if kwargs.get('model') else all_models
     outlist = []
     combinations = list(itertools.product(variables, scenarios, models))
     for comb in combinations:
          years = all_years.get(comb[1])
          final_attributes = map(lambda y: [*comb, y], years)
          for element in final_attributes:
               outlist.append(element)
     return list(outlist)

def get_dataset(ctx, prefix):
     # ctx: ['pr', 'historical', 'ACCESS1-0', 1950]
     filename = f"{prefix}/{ctx[0]}_day_BCSD_{ctx[1]}_r1i1p1_{ctx[2]}_{ctx[3]}.nc"
     logging.debug(f"filename: {filename}")
     return xr.open_dataset(filename, chunks = {'lat': 60, 'lon': 60})

def is_leap_year(year):
     logging.debug(f"year: {year}")
     # Poor man's leap year
     div_4 = True if  year % 4 == 0 else False
     div_100 = True if year % 100 == 0 else False
     return True if div_4 and not div_100 else False

def monthly_avg(dataset):
     return dataset.resample('1MS', dim='time', how='mean')

def calc_cumulative_pr(dataset):
     return dataset.resample('1YS', dim='time', how=np.sum)

def yearly_avg(dataset):
     return dataset.resample('1YS', dim='time', how='mean')

def cut_and_paste(arr):
     # eager_arr = arr.load().values
     split_raster = np.hsplit(arr, 2)
     pasted_raster = np.flipud(np.hstack((split_raster[1], split_raster[0])))
     return pasted_raster

def reshape(arr):
     out = np.empty_like(arr)
     eager_arr = arr.values
     logging.debug(f"eager_arr.shape: {eager_arr.shape}")
     if len(eager_arr.shape) == 3:
          for i in range(eager_arr.shape[0]):
               out[i, :, :] = np.squeeze(cut_and_paste(eager_arr[i, :, :]))
     else:
          out = cut_and_paste(eager_arr)
     return out

def create_new_dataset(filename, arr):
     nbands = arr.shape[0]
     raster = gdal.GetDriverByName('GTiff').Create(filename, ncols, nrows, nbands, gdal.GDT_Float32)
     raster.SetGeoTransform(geotransform)
     srs = osr.SpatialReference()
     srs.ImportFromEPSG(4326)
     raster.SetProjection( srs.ExportToWkt() )
     for nband in range(nbands):
          outBand = raster.GetRasterBand(nband + 1)
          outBand.WriteArray(np.squeeze(arr[nband, :, :]))
     raster = None
     return True

def calc_cdd(arr, axis, **kwargs):
     cdd = lambda data: len(list(filter(lambda x: ((x * 1.8) - 459.67) > 65.0, data)))
     return np.apply_along_axis(cdd, axis, arr)

def calc_hdd(arr, axis, **kwargs):
     cdd = lambda data: len(list(filter(lambda x: ((x * 1.8) - 459.67) < 65.0, data)))
     return np.apply_along_axis(cdd, axis, arr)
##############
# PROCESSING #
##############

# DOWNLOADING FILES

# Generating context - i.e. all files that need to be processed
contexts = get_context(scenario='historical')
logging.debug(f"contexts: {contexts}")

# Getting all distinct years from the whole context
contexts_years = sorted(list(set(map(lambda x: x[3], contexts))))
logging.debug(f"contexts_years: {contexts_years}")
# context: [['pr', 'historical', 'ACCESS1-0', 1950], ['pr', 'historical', 'BNU-ESM', 1950], ... ]

# Looping through years
for i, year in enumerate(contexts_years):
     context = [ctx for ctx in contexts if ctx[3] == year]
     logging.debug(f"context: {list(context)}")
     urls = list(map(lambda c: get_url(*c), context))
     logging.debug(f"urls: {urls}")
     # urls: ['http://192.168.1.42:8080/NEX-GDDP/BCSD/historical/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1950.nc', ... ]

     # Async downloading files
     try:
          pool = Pool()
          pool.map(download_file, urls)
          pool.close()
          pool.join()
     except:
          pass

     print("Finished downloading")
     # PROCESSING FILES
     logging.info("Getting datasets")
     # Gets datasets from file disk
     datasets = map(lambda ctx: get_dataset(ctx, download_prefix), context)
     target = list(zip(context, datasets))
     # logging.debug(f"target: {target}")


     # ACTUAL PROCESSING
     #
     # First, monthly averages.
     # This is calculated for all vars

     # for ctx, dataset in target:
     #      logging.info(f"Calculating monthly averages for ctx {ctx}")
     #      monthly = monthly_avg(dataset)
     #      data_array = np.squeeze(monthly.to_array())
     #                 # ^ consider extra dimensions
     #      out_raster_stack = reshape(data_array)
     #      filename = f"{file_prefix}/{ctx[0]}_{ctx[1]}_{ctx[2]}_{ctx[3]}_monthly_avg.tif"
     #      logging.debug(f"filename: {filename}")
     #      create_new_dataset(filename, out_raster_stack)

     # Now, onto the rest of the indicators
     logging.info("Calculating extra indicators")

     # First, average temperature (tasmax / tasmin) / 2
     logging.debug("tasavg")
     avg_temp_target = list(filter(lambda tgt: tgt[0][0] == "tasmax" or tgt[0][0] == "tasmin", target))
     # logging.debug(f"avg_temp_target: {avg_temp_target}")
     unique_models = list(set(map(lambda tgt: tgt[0][2], avg_temp_target)))
     logging.debug(f"unique_models: {unique_models}")
     target_by_model = [ [tgt for tgt in avg_temp_target if tgt[0][2] == model ] for model in unique_models ]
     for model_target in target_by_model:
          ctx = model_target[0][0]
          logging.info(f"Processing tasavg {ctx[1:]}")
          model_datasets = xr.merge(list(map(lambda tgt: tgt[1], model_target))).chunk({'lat': 180, 'lon': 180})
          model_datasets['tasavg'] = ((model_datasets['tasmax'] + model_datasets['tasmin'])/2 )
          monthly_tasavg = monthly_avg(model_datasets['tasavg'])
          filename = f"{file_prefix}/tasavg_{ctx[1]}_{ctx[2]}_{ctx[3]}_monthly_avg.tif"
          create_new_dataset(filename, reshape(monthly_tasavg))
          # CDD and HDD
          logging.debug("Calculating cdd and hdd")
          cdd = model_datasets['tasavg'].reduce(calc_cdd, dim='time')
          logging.debug(f"cdd: {cdd}")
          hdd = model_datasets['tasavg'].reduce(calc_hdd, dim='time')
          logging.debug(f"hdd: {hdd}")
          extra_vars = xr.concat([cdd, hdd], pd.Index(['cdd', 'hdd'], name = 'additional_indexes'))
          logging.debug(f"extra_vars: {extra_vars}")
logging.info("Done!")
