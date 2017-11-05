##########
# IMPORT #
##########

import os
import sys

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
from urllib import request
import dask

from multiprocessing import Pool

###############
# GLOBAL VARS #
###############
data_url = 'http://mymachine:8080' 
# data_url = 'http://nasanex.s3.amazonaws.com'
file_prefix = 'data'
download_prefix = 'downloads'

#############
# FUNCTIONS #
#############

def download_file(url):
     print(f"Downloading file at url: {url}")
     filename = download_prefix + '/' + str(url.split('/')[-1])
     u = request.urlopen(url)
     f = open(filename, 'wb')
     f.write(u.read())
     f.close()
     print(f"{filename} downloaded")
     
def get_file(variable, scenario, model, year, prefix = data_url):
     # /NEX-GDDP/BCSD/rcp85/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_rcp85_r1i1p1_inmcm4_2096.nc
     filename = f"{prefix}/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{str(year)}.nc"
     return filename

def get_context(**kwargs):
     all_scenarios = ['historical', 'rcp45', 'rcp85']
     all_models = ['ACCESS1-0', 'BNU-ESM', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'inmcm4']
     all_models = ['ACCESS1-0', 'BNU-ESM']
     # all_models = ['ACCESS1-0']
     all_vars = ['pr', 'tasmax', 'tasmin']
     all_years = {
          'historical': list(range(1950, 1981)),
          'rcp45': list(range(2006, 2101)),
          'rcp85': list(range(2006, 2101))
     }
     
     # all_years = {
     #      'historical': list(range(1950, 1981)),
     #      'rcp45': list(range(2006, 2101)),
     #      'rcp85': list(range(2006, 2101))
     # }

     # Override for smaller calculations

     
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
     print(f"filename: {filename}")
     return xr.open_dataset(filename, chunks = {'lat': 60, 'lon': 60})

def is_leap_year(year):
     print(f"year: {year}")
     # Poor man's leap year
     div_4 = True if  year % 4 == 0 else False
     div_100 = True if year % 100 == 0 else False
     return True if div_4 and not div_100 else False

def monthly_avg(dataset):
     return dataset.resample('1MS', dim='time', how='mean')

def yearly_avg(dataset):
     return dataset.resample('1YS', dim='time', how='mean')

def cut_and_paste(arr):
     eager_arr = arr.load().values
     print("Eager array loading")
     split_raster = np.hsplit(eager_arr, 2)
     pasted_raster = np.flipud(np.hstack((split_raster[1], split_raster[0])))
     return pasted_raster

##############
# PROCESSING #
##############

# DOWNLOADING FILES
# Needs catch for unavailable files
contexts = get_context(variable='tasmax', scenario='historical')
print(f"contexts: {contexts}")
     
contexts_years = sorted(list(set(map(lambda x: x[3], contexts))))
print(f"contexts_years: {contexts_years}")
# context: [['pr', 'historical', 'ACCESS1-0', 1950], ['pr', 'historical', 'BNU-ESM', 1950], ... ]

for i, year in enumerate(contexts_years):
     context = [ctx for ctx in contexts if ctx[3] == year]
     print(f"context: {list(context)}")
     urls = list(map(lambda c: get_file(*c), context))
     print(f"urls: {urls}")
     # urls: ['http://192.168.1.42:8080/NEX-GDDP/BCSD/historical/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1950.nc', ... ]

     pool = Pool()
     pool.map(download_file, urls)
     pool.close()
     pool.join()

     # PROCESSING FILES
     print("Getting datasets")
     datasets = map(lambda ds: get_dataset(ds, download_prefix), context)
     target = list(zip(context, datasets))
     print(f"target: {target}")
     for ctx, dataset in target:
          # ACTUAL PROCESSING
          print("Calculating monthly averages")
          monthly = monthly_avg(dataset)
          print("Loading into memory")
          data_array = np.squeeze(monthly.to_array())
                     # ^ consider extra dimensions
          out_raster_stack = np.empty_like(data_array)

          print("data_array.shape")
          print(data_array.shape)

          filename = f"{file_prefix}/{ctx[0]}_{ctx[1]}_{ctx[2]}_{ctx[3]}_monthly_avg.tif"
          print(f"filename: {filename}")
     
          for i in range(data_array.shape[0]):
               print(f"Processing band {str(i)}")
               raster = np.squeeze(data_array[i, :, :])
               print(f"Cutting and pasting")
               reproj_array = cut_and_paste(raster)
               out_raster_stack[i, :, :] = np.squeeze(reproj_array)

          xmin, ymin, xmax, ymax = [-180, -90, 180, 90]
          nrows, ncols = np.shape(out_raster_stack[0, :, :])
          print(f"nrows: {nrows}")
          print(f"ncols: {ncols}")
          xres = (xmax - xmin) / float(ncols)
          yres = (ymax - ymin) / float(nrows)
          geotransform = (xmin, xres, 0, ymax, 0, -yres)
          output_raster = gdal.GetDriverByName('GTiff').Create(filename, ncols, nrows, data_array.shape[0], gdal.GDT_Float32)
          output_raster.SetGeoTransform(geotransform)
          srs = osr.SpatialReference()
          srs.ImportFromEPSG(4326)
          output_raster.SetProjection( srs.ExportToWkt() )
          for nband in range(out_raster_stack.shape[0]):
               outBand = output_raster.GetRasterBand(nband + 1)
               outBand.WriteArray(np.squeeze(out_raster_stack[nband, :, :]))
          output_raster = None
          os.remove(f"{download_prefix}/{ctx[0]}_day_BCSD_{ctx[1]}_r1i1p1_{ctx[2]}_{ctx[3]}.nc")
print("Done!")
