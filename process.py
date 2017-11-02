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

data_url = 'http://192.168.1.42:8080'
file_prefix = 'data'

#############
# FUNCTIONS #
#############

def download_file(url):
     print(f"Downloading file at url: {url}")
     filename = file_prefix + '/' + str(url.split('/')[-1])
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
     # all_models = ['ACCESS1-0', 'BNU-ESM', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'inmcm4']
     all_models = ['ACCESS1-0', 'BNU-ESM']
     all_vars = ['pr', 'tasmax', 'tasmin']
     all_years = {
          'historical': list(range(1950, 1951)),
          'rcp45': list(range(2006, 2101)),
          'rcp85': list(range(2006, 2101))
     }
     
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

def cut_and_paste(arr):
    cut = np.split(arr, 2, axis = 1)
    print(cut)
    paste = np.concatenate((cut[1], cut[0]), axis = 'lon')
    return paste

##############
# PROCESSING #
##############

# DOWNLOADING FILES
# Needs catch for unavailable files
context = get_context(variable='pr', scenario='historical')
print(f"context: {context}")
# context: [['pr', 'historical', 'ACCESS1-0', 1950], ['pr', 'historical', 'BNU-ESM', 1950], ... ]

urls = list(map(lambda c: get_file(*c), context))
print(f"urls: {urls}")
# urls: ['http://192.168.1.42:8080/NEX-GDDP/BCSD/historical/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1950.nc', ... ]

pool = Pool()
pool.map(download_file, urls)
pool.close()
pool.join()

# PROCESSING FILES
print("Getting datasets")
datasets = map(lambda ds: get_dataset(ds, file_prefix), context)
target = list(zip(context, datasets))
print(f"target: {target}")
for ctx, dataset in target:
     monthly = monthly_avg(dataset)
     data_array = np.squeeze(monthly.to_array())
     # print(data_array)
     out_raster_stack = np.empty_like(data_array)

     for i in range(data_array.shape[0]):
          raster = np.squeeze(data_array[i, :, :])
          raster_array = raster.load()
          print(raster_array)
          print(raster_array.shape)
          # new_raster = cut_and_paste(raster)
          out_raster_stack[i, :, :] = np.squeeze(raster_array)

     print(ctx)
     filename = f"{file_prefix}_{ctx[0]}_{ctx[1]}_{ctx[2]}_{ctx[3]}_monthly_avg.tif"
     print(ctx)
     xmin,ymin,xmax,ymax = [-180, -90, 180, 90]
     nrows,ncols = np.shape(out_raster_stack[0, :, :])
     xres = (xmax-xmin)/float(ncols)
     yres = (ymax-ymin)/float(nrows)
     geotransform=(xmin,xres,0,ymax,0, -yres)
     output_raster = gdal.GetDriverByName('GTiff').Create(filename, ncols, nrows, 19, gdal.GDT_Float32)
     output_raster.SetGeoTransform(geotransform)
     srs = osr.SpatialReference()
     srs.ImportFromEPSG(4326)
     output_raster.SetProjection( srs.ExportToWkt() )
     for nband in range(out_raster_stack.shape[0]):
          outBand = output_raster.GetRasterBand(nband + 1)
          outBand.WriteArray(np.squeeze(out_raster_stack[nband, :, :]))
     output_raster = None     

print("Done!")

#     processed_arrays = np.stack((
#         tmax90F,
#         tmax95F,
#         tmax100F,
#         icing_days,
#         frost_days,
#         pr2in,
#         pr3in,
#         pr4in,
#         cdd,
#         cwd,
#         tmax5day,
#         tmin5day,
#         tmax99p,
#         tmax1p,
#         tmin99p,
#         tmin1p,
#         pr99p,
#         pr1p,
#         prmaxday
#     ), axis=0)
    
#     out_raster_stack = np.empty_like(processed_arrays)

#     for i in range(processed_arrays.shape[0]):
#         raster = np.squeeze(processed_arrays[i, :, :])
#         new_raster = np.flipud(cut_and_paste(raster))
#         out_raster_stack[i, :, :] = np.squeeze(new_raster)
        
#     filename = scenario + "_" + model + "_" + year + ".tif"
#     xmin,ymin,xmax,ymax = [-180, -90, 180, 90]
#     nrows,ncols = np.shape(out_raster_stack[0, 0, :, :])
#     xres = (xmax-xmin)/float(ncols)
#     yres = (ymax-ymin)/float(nrows)
#     geotransform=(xmin,xres,0,ymax,0, -yres)
#     output_raster = gdal.GetDriverByName('GTiff').Create(filename, ncols, nrows, 19, gdal.GDT_Float32)
#     output_raster.SetGeoTransform(geotransform)
#     srs = osr.SpatialReference()
#     srs.ImportFromEPSG(4326)
#     output_raster.SetProjection( srs.ExportToWkt() )

#     for nband in range(out_raster_stack.shape[0]):
#         outBand = output_raster.GetRasterBand(nband + 1)
#         outBand.WriteArray(np.squeeze(out_raster_stack[nband, 0, :, :]))

#     output_raster = None


# years = ["1951", "1952"]
# for year in years:
#     calc("historical", "ACCESS1-0", year, prefix=prefix)

# print("Success.")
