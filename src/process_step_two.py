import itertools
from multiprocessing import Pool
from urllib import request
from osgeo import osr
import gdal
import arrow
import numpy as np

###############
# GLOBAL VARS #
###############

gdal.UseExceptions()
data_url = 'http://mymachine:8080'
file_prefix = 'data'
download_prefix = 'downloads'
all_scenarios = ['historical', 'rcp45', 'rcp85']
all_models = ['ACCESS1-0', 'BNU-ESM', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'inmcm4']
#all_models = ['ACCESS1-0', 'BNU-ESM']
# all_models = ['ACCESS1-0']
all_vars = ['pr', 'tasmax', 'tasmin']
all_years = {
    'historical': list(range(1950, 1981)),
    'rcp45': list(range(2006, 2101)),
    'rcp85': list(range(2006, 2101))
}

months = ['Jan', 'Feb', 'March', 'April', 'May', 'June', 'July', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']

nrows, ncols = 720, 1440
xmin, ymin, xmax, ymax = [-180, -90, 180, 90]
xres = (xmax - xmin) / float(ncols)
yres = (ymax - ymin) / float(nrows)
geotransform = (xmin, xres, 0, ymax, 0, -yres)

########################
# FUNCTION DEFINITIONS #
########################

def get_file(variable, scenario, model, year, prefix = data_url):
     # /NEX-GDDP/BCSD/rcp85/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_rcp85_r1i1p1_inmcm4_2096.nc
     filename = f"{prefix}/processed/{variable}_{scenario}_{model}_{str(year)}_monthly_avg.tif"
     return filename

def download_file(url):
     print(f"Downloading file at url: {url}")
     filename = download_prefix + '/' + str(url.split('/')[-1])
     u = request.urlopen(url)
     f = open(filename, 'wb')
     f.write(u.read())
     f.close()
     print(f"{filename} downloaded")

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
     filename = f"{prefix}/{ctx[0]}_{ctx[1]}_{ctx[2]}_{str(ctx[3])}_monthly_avg.tif"
     print(f"filename: {filename}")
     return gdal.Open(filename)

##############
# PROCESSING #
##############
VAR = 'tasmax'
SCENARIO = 'historical'


contexts = get_context(variable=VAR, scenario=SCENARIO)
print(f"contexts: {contexts}")
contexts_years = sorted(list(set(map(lambda x: x[3], contexts))))

#############################
# PROCESSING BATCH IN YEARS #
#############################

for i, year in enumerate(contexts_years):
     context = [ctx for ctx in contexts if ctx[3] == year]
     print(f"context: {list(context)}")
     urls = list(map(lambda c: get_file(*c), context))
     print(f"urls: {urls}")
     # Pooling downloads - need to setup logging
     pool = Pool()
     pool.map(download_file, urls)
     pool.close()
     pool.join()
     print("Getting datasets")
     datasets = map(lambda ds: get_dataset(ds, download_prefix), context)
     target = list(zip(context, datasets))
     print(f"target: {target}")

     # One raster for each month
     for mo in range(12):
         print(f"Processing {months[mo]} {year}")
         output_filename = f"{file_prefix}/{VAR}_{SCENARIO}_{year}_{mo + 1}_{months[mo]}.tif"
         output_raster = gdal.GetDriverByName('GTiff').Create(
             output_filename, ncols, nrows, len(target) + 3, gdal.GDT_Float32
         )
         output_raster.SetGeoTransform(geotransform)
         srs = osr.SpatialReference()
         srs.ImportFromEPSG(4326)
         output_raster.SetProjection(srs.ExportToWkt())

         for nband, model in enumerate(all_models):
             current_context = filter(lambda tgt: tgt[0][2] == model, target)
             current_dataset = list(current_context)[0][1]
             current_band = current_dataset.GetRasterBand(mo + 1)
             output_raster_band = output_raster.GetRasterBand(nband + 1)
             output_raster_band.SetDescription(model)
             output_raster_band.WriteArray(current_band.ReadAsArray())
         output_raster.SetMetadataItem(
             'TIFFTAG_DATETIME',
             arrow.get(year, mo + 1, 1).format().replace(' ', 'T')
         )

         data_array = output_raster.ReadAsArray()[0:12, :, :]
         print(data_array.shape)

         avg = np.average(data_array, 0)
         # Very slow - do we implement interpolation or not?
         # percentile_25th = np.nanpercentile(data_array, 25, 0)
         # percentile_75th = np.nanpercentile(data_array, 75, 0)
         # print(percentile_25th)
         output_raster = None

         
         
#################
# DECADAL MEANS #
#################

################
# 30 YEAR MEAN #
################
