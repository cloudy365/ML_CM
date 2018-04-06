from zyz_core import os, np, h5py
import sys


def read_merged_MODfile(h5f_file, n, cldorclr, sfc_type=None):
    
    """
    Read flags and radiances from a merged MOD0235 <h5f_file>.
    
    Crop at most <n> 21*21 <sfc_type> sub-images.
    
    Other default settings:
        flag_determine = 1
        flag_daynight = 1
        flag_sunglint = 1
        flag_snowice = 1
        
    Output:
        X is in shape (n, 21, 21, 8), which includs 5 spectral channels and SZA, VZA, and RAZ.
        y is in shape (n,), which is the cloud classification (0 for cloudy and 1 for clear) of the central pixel.
    """
    
    dict_cldclr = {'cloudy' : 0.0, 'clear' : 3.0 }
    h5f = h5py.File(h5f_file, 'r')
    
    flag_snowice = h5f['flag_snowice'][:] # sfc_type = h5f['flag_surface'][:]
    flag_determine = h5f['flag_determine'][:]
    flag_daynight = h5f['flag_daynight'][:]
    flag_sunglint = h5f['flag_sunglint'][:]
    
    av_idx_y, av_idx_x = np.where((flag_snowice==1)&(flag_determine==1)&(flag_daynight==1)&(flag_sunglint==1))
    num_samples = len(av_idx_x)
    if num_samples*0.001 > n:
        n = int(num_samples*0.001)
    
       
    X = []
    y = []
    cnt = 0
    if num_samples >= 500:
       
        rads = h5f['radiance'][:]
        geometry = []
        geometry.append( h5f['flag_sza'][:] )
        geometry.append( h5f['flag_vza'][:] )
        geometry.append( h5f['flag_raz'][:] )
        geometry = np.rollaxis(np.array(geometry), 0, 3)
        tags = h5f['tag_cloud'][:]
            
        # specified the random sampling range.
        idx = range(num_samples)
        np.random.shuffle(idx)
        
        for i_idx in idx:
            idx_y = av_idx_y[i_idx]
            idx_x = av_idx_x[i_idx]            
            tmp_y = tags[idx_y, idx_x]
            
            if tmp_y == dict_cldclr[cldorclr]:
                tmp_X = rads[idx_y-10:idx_y+11, idx_x-10:idx_x+11, :]
                if tmp_X.shape != (21, 21, 5):
                    continue

                tmp_geo = geometry[idx_y-10:idx_y+11, idx_x-10:idx_x+11, :]
                tmp_X_combined = np.concatenate((tmp_X, tmp_geo), axis=2)
                X.append(tmp_X_combined)
                y.append(int(tmp_y))
                    
                cnt += 1
                if cnt == n:
                    break
    
    if cnt > 0:
        print ">> {:7d} samples fulfill the specified criterion, {:4d} {} samples have been retrieved.".format(num_samples, cnt, cldorclr)
    X = np.array(X)
    y = np.array(y)
    return X, y, cnt


def wrapper(CLDCLR, num_batch):
    
    """
    Snow/ice samples:
        get at most 200 samples from each MO0235 merge result,
        use MPI to generate cloudy and clear training dataset at the same time.
    """
    
    MAX_NUM_FROM_A_FILE = 200
    SAMPLE_NUM_SAVED_PER_FILE = 10000
    DATA_FOLDER_PATH = "/u/sciteam/smzyz/scratch/results/MO0235_merge/2010"
    MERGED_FILES = os.listdir(DATA_FOLDER_PATH)
    np.random.shuffle(MERGED_FILES)
    RESULT_PATH = "/u/sciteam/smzyz/scratch/results/MOD_training/_21X21/noglint"


    X_all = np.zeros((1, 21, 21, 8))
    y_all = np.array([np.nan])
    cnt_samples = 0
    for i, ifile in enumerate(MERGED_FILES):

        if ifile.endswith('h5'):
            h5f_file = os.path.join(DATA_FOLDER_PATH, ifile)
            try:
                X, y, cnt = read_merged_MODfile(h5f_file=h5f_file, n=MAX_NUM_FROM_A_FILE, cldorclr=CLDCLR)
            except Exception as err:
                print ">> Error raises: {}".format(err)

            if cnt == 0:
                continue
            X_all = np.vstack((X_all, X))
            y_all = np.append(y_all, y)
            cnt_samples += cnt

            if cnt_samples >= SAMPLE_NUM_SAVED_PER_FILE:
                print ">  Writing training dataset: {}_batch_{}.h5".format(CLDCLR, num_batch)
                print ">  {}/{} merged files have been processed.".format(i, len(MERGED_FILES))
                file_out = os.path.join(RESULT_PATH, "{}_batch_{}.h5".format(CLDCLR, num_batch))
                with h5py.File(file_out, 'a') as h5f:
                    h5f.create_dataset('/X', data=X_all[1:], compression='gzip')
                    h5f.create_dataset('/y', data=y_all[1:], dtype='i8', compression='gzip')

                # reset counting
                num_batch += 1
                cnt_samples = 0
                X_all = np.zeros((1, 21, 21, 8))
                y_all = np.array([np.nan ])
        

if __name__ == '__main__':
    
    import mpi4py.MPI as MPI
    
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    
    if comm_rank == 0:
        wrapper('cloudy', 0)
        print ">> Cloudy part finished."
    elif comm_rank == 1:
        wrapper('clear', 0)
        print ">> Clear part finished."


    
