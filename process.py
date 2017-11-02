import os
import sys

#import boto3
#import botocore

import gdal
from osgeo import osr
import netCDF4

import numpy as np
import xarray as xr
from itertools import groupby
import matplotlib.pyplot as plt
import numba as nb

file_prefix = 'http://192.168.1.36:8081'

def get_file(variable, scenario, model, year, **kwargs):
     filename = var + "_day_BCSD_" + scenario + "_r1i1p1_" + model + "_" + year + ".nc"
     filename = kwargs.get("prefix") + "/" + filename if kwargs.get('prefix') else filename
     print(filename)
     dataset = xr.open_dataset(filename)
     return dataset

 
# prefix= "/Volumes/TOSHIBA EXT/nexgddp/data"
# def get_file(var, scenario, model, year, **kwargs):
#     filename = var + "_day_BCSD_" + scenario + "_r1i1p1_" + model + "_" + year + ".nc"
#     filename = kwargs.get("prefix") + "/" + filename if kwargs.get('prefix') else filename
#     print(filename)
#     dataset = xr.open_dataset(filename)
#     return dataset

# prefix= "data"

# def get_file(var, scenario, model, year, **kwargs):
#     filename = var + "_day_BCSD_" + scenario + "_r1i1p1_" + model + "_" + year + ".nc"
#     filename = kwargs.get("prefix") + "/" + filename if kwargs.get('prefix') else filename
#     print(filename)
#     dataset = xr.open_dataset(filename)
#     return dataset

# def calc_cdd(data_values):
#     cdd_count = 0
#     max_count = 0
#     for value in data_values:
#         if value < 0.0000029398:
#             cdd_count = cdd_count + 1
#             if cdd_count > max_count:
#                 max_count = cdd_count
#         else:
#             cdd_count = 0
#     return max_count

# def calc_cwd(data_values):
#     cwd_count = 0
#     max_count = 0
#     for value in data_values:
#         if value >= 0.0000029398:
#             cwd_count = cwd_count + 1
#             if cwd_count > max_count:
#                 max_count = cwd_count
#         else:
#             cwd_count = 0
#     return max_count

# def moving_average(a, n=5):
#     ret = np.cumsum(a, dtype=float)
#     ret[n:] = ret[n:] - ret[:-n]
#     return ret[n - 1:] / n

# def cut_and_paste(raster):
#     cut = np.split(np.squeeze(raster), 2, axis = 1)
#     paste = np.concatenate((cut[1], cut[0]), axis = 1)
#     return paste

# def calc(scenario, model, year, **kwargs):
#     print("Getting files for year " + year)
#     dataset_tasmin = get_file("tasmin", scenario, model, year, prefix=kwargs.get('prefix'))
#     dataset_tasmax = get_file("tasmax", scenario, model, year, prefix=kwargs.get('prefix'))
#     dataset_pr     = get_file("pr", scenario, model, year, prefix=kwargs.get('prefix'))
    
#     print("Extracting data")
#     array_tasmin = dataset_tasmin.to_array()
#     array_tasmax = dataset_tasmax.to_array()
#     array_pr     = dataset_pr.to_array()
    
#     print("Index calc")
    
#     print("tmax90F")
#     tmax90F = np.apply_along_axis(lambda x: (x > 305.372).sum(), 1, array_tasmax)
    
#     print("tmax95F")
#     tmax95F = np.apply_along_axis(lambda x: (x > 308.150).sum(), 1, array_tasmax)
    
#     print("tmax100F")
#     tmax100F = np.apply_along_axis(lambda x: (x > 310.928).sum(), 1, array_tasmax)

#     print("icing_days")
#     icing_days = np.apply_along_axis(lambda x: (x < 273.150).sum(), 1, array_tasmax)
    
#     print("frost_days")
#     frost_days = np.apply_along_axis(lambda x: (x < 273.150).sum(), 1, array_tasmin)
    
#     print("pr2in")
#     pr2in = np.apply_along_axis(lambda x: (x > 0.0005879).sum(), 1, array_pr)
    
#     print("pr3in")
#     pr3in = np.apply_along_axis(lambda x: (x > 0.0008819).sum(), 1, array_pr)
    
#     print("pr4in")
#     pr4in = np.apply_along_axis(lambda x: (x > 0.0011759).sum(), 1, array_pr)
    
#     print("cdd")
#     cdd = np.apply_along_axis(lambda x: calc_cdd(x), 1, array_pr)
    
#     print("cwd")
#     cwd = np.apply_along_axis(lambda x: calc_cwd(x), 1, array_pr)
    
#     print("tmax5day")
#     tasmax_moving_averages = np.apply_along_axis(moving_average, 1, array_tasmax)
#     tmax5day = np.apply_along_axis(np.amax, 1, tasmax_moving_averages)
    
#     print("tmin5day")
#     tasmin_moving_averages = np.apply_along_axis(moving_average, 1, array_tasmin)
#     tmin5day = np.apply_along_axis(np.amax, 1, tasmin_moving_averages)
    
#     print("tmax99p")
#     tmax99p = np.apply_along_axis(lambda x: np.percentile(x, 99), 1, array_tasmax)
    
#     print("tmax1p")
#     tmax1p = np.apply_along_axis(lambda x: np.percentile(x, 1), 1, array_tasmax)
    
#     print("tmin99p")
#     tmin99p = np.apply_along_axis(lambda x: np.percentile(x, 99), 1, array_tasmin)
    
#     print("tmin1p")
#     tmin1p = np.apply_along_axis(lambda x: np.percentile(x, 1), 1, array_tasmin)
    
#     print("pr99p")
#     pr99p = np.apply_along_axis(lambda x: np.percentile(x, 99), 1, array_pr)
    
#     print("pr1p")
#     pr1p = np.apply_along_axis(lambda x: np.percentile(x, 1), 1, array_pr)
    
#     print("prmaxday")
#     prmaxday = np.apply_along_axis(lambda x: np.amax(x) * 86400, 1, array_pr)
    
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
