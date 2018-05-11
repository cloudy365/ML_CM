

from my_module import np, os, h5py, tqdm
np.warnings.filterwarnings("ignore")
from my_module.data.comm import save_data_hdf5
from sort_mod35 import test



def latslons_to_idxs(lats, lons, num):
    """
    An updated key function that determines the lat/lon indexes for a given resolution map based on the input lats and lons.
    This function is exactly the same as latlon_to_idx but working on the whole lat/lon array at the same time.
    num: 1/resolution
    """

    lats_int = lats.astype('int32')
    lons_int = lons.astype('int32')
    lats_dec = lats - lats_int
    lons_dec = lons - lons_int

    # Latitude
    lats_idx = (90-lats_int) * num - (lats_dec*num).astype('int32')
    lats_idx[lats>=0] -= 1

    # Longitude
    lons_idx = (180+lons_int) * num + (lons_dec*num).astype('int32')
    lons_idx[lons<0] -= 1
    np.place(lons_idx, lons_idx==360*num, 0)

    return lats_idx, lons_idx
    
    

def times_gen(itype):
    """
    Generate times for processing, itype could be 1 or 2.
    itype == 1: Generate iyr+iday for MPI processing;
    itype == 2: Generate ihr+imin for granule processing, which calls once at the very begining of the Main function.
    """
    times = []
    if itype == 1:
        for iyr in range(2000, 2016):
            for iday in range(1, 367):
                tmp = "{}{}".format(iyr, str(iday).zfill(3))
                times.append(tmp)
                
    elif itype == 2:
        for ihr in range(24):
            for imin in range(0, 60, 5):
                tmp = "{}{}".format(str(ihr).zfill(2), str(imin).zfill(2))
                times.append(tmp)

    return np.array(times)





mod03 = '/u/sciteam/smzyz/scratch/data/MODIS/MOD03_daily/2015/MOD03.A2015001.006.h5'#'/Users/yizhe/MOD03.A2015001.006.h5'
mod35 = '/u/sciteam/smzyz/scratch/data/MODIS/MOD35_daily/2015/MOD35_L2.A2015001.061.h5'#'/Users/yizhe/MOD35_L2.A2015001.061.h5'
output_folder = '/u/sciteam/smzyz'


def main(mod35, mod03, output_folder):
    """
    This is the main function, wrapping everything together so that user just need to specify the paths of 
    MOD021KM_XX_daily and MOD03_daily data and the output path.
    
    This code supports VIS bands for now and will be updated for other bands later (if necessary).
    """
        
        
    """
    Initialization
    """
    # CONSTANT PARAMETERS
    SPATIAL_RESOLUTION = 0.5
    NUM_POINTS = 1 / SPATIAL_RESOLUTION
    NUM_LATS = int(180 / SPATIAL_RESOLUTION)
    NUM_LONS = int(360 / SPATIAL_RESOLUTION)
    VZA_MAX = 40
    
    
    # Initialize organized MOD02/03 data interfaces
    mod_date = mod03.split('.')[1]
    try:
        mod35 = h5py.File(mod35, 'r')
        mod03 = h5py.File(mod03, 'r')
    except IOError as err:
        print ">> IOError, cannot access {}".format(mod_date)
    
    
    
    # Initialize output arrays and output hdf5 file
    daily_cld = np.zeros((NUM_LATS, NUM_LONS))
    daily_tot = np.zeros((NUM_LATS, NUM_LONS))
    daily_h5f_out = os.path.join(output_folder, '{}.h5'.format(mod_date))
    
    
    
    """
    Main loop
    """
    # The following part sorts the radiances into the corresponding lat/lon bins
    times = times_gen(2)[:]
    for i in tqdm(range(len(times))):
    # for i in range(29):
        itime = times[i]
        try:
            sza = mod03['{}/SolarZenith'.format(itime)][:, :]/100.
            vza = mod03['{}/SensorZenith'.format(itime)][:, :]/100.
        except KeyError as err:
            print ">> KeyError, cannot access {}.{}".format(mod_date, itime)
            continue
            
            
        # GRANULE-LEVEL CHECK is applied here,
        # 0 <= SZA < 90.0  and  0 <= VZA < 40.0
        #
        # PIXEL-LEVEL CHECK is applied in the fortran code,
        # descending node (lat < lat_previous) 
        # valid lat/lon   (lat != -999 and lon != -999 and idx_lat/lon are valid)
        # valid radiance  (0 < rad <= rad_max)
        valid_y, valid_x = np.where((sza>=0)&(sza<90.0)&(vza>=0)&(vza<VZA_MAX))
        valid_num = len(valid_x)
        if valid_num == 0:
            continue
            
        cld = mod35['{}/Cloud_Mask'.format(itime)][:, :, :2]
        np.place(cld[:, :, 1], cld[:, :, 1]==1, 3)
        np.place(cld[:, :, 1], cld[:, :, 1]==2, 3)
        
        
        
        # Read Lats/Lons
        lats = mod03['{}/Latitude'.format(itime)][:]
        lons = mod03['{}/Longitude'.format(itime)][:]
        
        
        # Calculate lat/lon indexes of all sample.
        lats_idx, lons_idx = latslons_to_idxs(lats, lons, NUM_POINTS)
        
        
        # Call main fortran subroutine to sort granule samples into lat/lon grids.
        daily_cld, daily_tot = test(len(valid_x), valid_x, valid_y, \
        len(lats_idx), lats, lons, lats_idx, lons_idx, \
        cld, \
        daily_cld, daily_tot)
        
        
    """
    Save final arrays
    """
    save_data_hdf5(daily_h5f_out, '/cloud_sum', daily_cld)
    save_data_hdf5(daily_h5f_out, '/total_sum', daily_tot)



main(mod35, mod03, output_folder)
