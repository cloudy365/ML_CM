


from my_module import np, os, tqdm, sys, time
import csv
import shutil
import ssl
import urllib2
import signal



def signal_handler(signum, frame):
    raise Exception("Timed out!")


USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')



def url_retrieve(src, dst):
    """
    Return 0 if an error occur otherwise Return 1.
    """
    
    url = src
    headers = { 'user-agent' : USERAGENT }
    headers['Authorization'] = 'Bearer ' + "59451810-53C9-11E8-9F28-C61EAE849760"
    CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    
    
    #signal.signal(signal.SIGALRM, signal_handler)
    #signal.alarm(60)   # Ten seconds

    try:
        fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
        shutil.copyfileobj(fh, dst)
    except urllib2.HTTPError as e:
        print '>> HTTPError: {}'.format(url)
    except urllib2.URLError as e:
        print '>> URLError: {}'.format(url)

    


def main(iyr, iday):
    iday = str(iday).zfill(3)
    
    # Set up download folder (dst_parent_folder)
    dst_parent_folder = "/u/sciteam/smzyz/scratch/data/MODIS/MOD35/{}".format(iyr) #"/u/sciteam/smzyz/scratch/data/MODIS/MOD35/{}".format(iyr)
    dst_daily_folder = os.path.join(dst_parent_folder, iday)
    try:
        # print ">> log: {}".format(dst_daily_folder)
        os.mkdir(dst_daily_folder)
    except OSError as err:
        pass
    
    
    # Set up server folder (src_parent_folder)
    src_parent_folder = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD35_L2/{}'.format(iyr)
    src_daily_folder = os.path.join(src_parent_folder, iday)
    
    
    # 1) Download daily .csv file from (src_parent_folder) to (dst_parent_folder)
    #    e.g., src: https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD35_L2/2000/300.csv
    #          dst: /u/sciteam/smzyz/scratch/data/MODIS/MOD35/2000/300.csv
    src_csv_file = "{}/{}.csv".format(src_parent_folder, iday)
    dst_csv_file = "{}/{}.csv".format(dst_parent_folder, iday)
    with open(dst_csv_file, 'w+b') as out:
        url_retrieve(src_csv_file, out)
    
    
    
    # 2) Read csv file and generate a download file list by cross-referencing existed MOD35 files
    files_new = []
    files_server = [ f['name'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
    files_local = os.listdir(dst_daily_folder)
    sizes_server = [ f['size'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
    sizes_local = [os.path.getsize(os.path.join(dst_daily_folder, ifile)) for ifile in files_local]
    
    for ifile, isize in zip(files_server, sizes_server):
        if (ifile in files_local):
            # Check file size
            idx = files_local.index(ifile)
            if int(sizes_local[idx]) == int(isize):
                continue
            else:
                # print isize, sizes_local[idx] # for debug
                files_new.append(ifile)
        else:
            files_new.append(ifile)



    if len(files_new) == 0:
        return
    else:
        src_file_list = ["{}/{}".format(src_daily_folder, f) for f in files_new]
        dst_file_list = ["{}/{}".format(dst_daily_folder, f) for f in files_new]
        
    
        # 3) Download
        print ">> Download: {}: {} files".format(dst_daily_folder, len(files_new))
        #for i in tqdm(range(len(src_file_list))):  # for debug
        for i in range(len(src_file_list)):
            isrc = src_file_list[i]
            idst = dst_file_list[i]
            with open(idst, 'w+b') as out:
                url_retrieve(isrc, out)
            
            
            
def times_gen(itype):
    """
    Generate times for processing, itype could be 1 or 2.
    itype == 1: Generate iyr+iday for MPI processing;
    itype == 2: Generate ihr+imin for granule processing, which calls once at the very begining of the Main function.
    """
    times = []
    if itype == 1:
        for iyr in range(2013, 2014):
            for iday in range(1, 367):
                tmp = "{}{}".format(iyr, str(iday).zfill(3))
                times.append(tmp)
                
    elif itype == 2:
        for ihr in range(24):
            for imin in range(0, 60, 5):
                tmp = "{}{}".format(str(ihr).zfill(2), str(imin).zfill(2))
                times.append(tmp)

    return np.array(times)





if __name__ == "__main__":
    import mpi4py.MPI as MPI
    
    # MPI initialization
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    

    # Get total number of PE
    NUM_CORES = int(sys.argv[1])


    # Main iteration
    times = times_gen(1)[:]
    
    for idd in range(0, len(times), NUM_CORES):
        
        try:
            itime = times[idd+comm_rank]
            main(int(itime[:4]), int(itime[4:]))
        except Exception as err:
            print ">> err: {}".format(err)
            continue
