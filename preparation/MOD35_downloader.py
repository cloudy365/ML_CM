


"""
LOG: 2018.05.11
This script is used for downloading large-scale MOD35 data, which can be modified to download any other MODIS data.
It uses urllib2 and shutil to set up the download pipline and signal to watch the download process. It is a localized version of
the LAADS DAAC code: https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/


One main issue remaining is that it is not clear how many download connections can be set simultantaneously. Additionally, downloaded files
have not been checked sizes, which is important and should be implemented in the future.
"""





from my_module import np, os, tqdm, sys
import csv
import shutil
import ssl
import urllib2
import mpi4py.MPI as MPI
import signal



# Don't know whether it is needed.
USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')



# Raises exception when download got stucked.
def signal_handler(signum, frame):
    raise Exception("Timed out!")

    
    
# Main function of download one file.
# Thit function actually could raise several kinds of errors, including:
# urllib2.HTTPError
# urllib2.URLError
# OutOfTime error defined by signal_handler.
def url_retrieve(src, dst):
    url = src
    headers = { 'user-agent' : USERAGENT }
    headers['Authorization'] = 'Bearer ' + "59451810-53C9-11E8-9F28-C61EAE849760"
    CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    
    
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(120)   # 120 seconds

    
    fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
    shutil.copyfileobj(fh, dst)
    #except urllib2.HTTPError as e:
    #    print '>> HTTPError: {}'.format(url)
    #except urllib2.URLError as e:
    #    print '>> URLError: {}'.format(url)

    


# Main function of download all granules within a particular day.
def main(iyr, iday):
    iday = str(iday).zfill(3)
    
    # Set up download folder (dst_parent_folder)
    dst_parent_folder = "/u/sciteam/smzyz/scratch/data/MODIS/MOD35/{}" .format(iyr)
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
    try:
        with open(dst_csv_file, 'w+b') as out:
            url_retrieve(src_csv_file, out)
    except Exception as err:
        print ">> Err: cannot download {}, move on to next day.".format(src_csv)
        return
    

    # 2) Read csv file and generate a download file list by cross-referencing existed MOD35 files
    files_server = [ f['name'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
    # sizes_server = [ f['size'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
    files_local = os.listdir(dst_daily_folder)
    # sizes_local = [os.path.getsize(os.path.join(dst_daily_folder, ifile)) for ifile in files_local]
    files_new = [ifile for ifile in files_server if ifile not in files_local]

    if len(files_new) == 0:
        return
    else:
        src_file_list = ["{}/{}".format(src_daily_folder, f) for f in files_new]
        dst_file_list = ["{}/{}".format(dst_daily_folder, f) for f in files_new]
        
    
        # 3) Download
        print ">> Download: {}: {} files".format(dst_daily_folder, len(files_new))
        for i in tqdm(range(len(src_file_list))):
            isrc = src_file_list[i]
            idst = dst_file_list[i]
            try:
                with open(idst, 'w+b') as out:
                    url_retrieve(isrc, out)
            except Exception as err:
                print ">> Err: cannot download {}, move on to next granule.".format(isrc)
                continue
           


def times_gen(itype):
    """
    Generate times for processing, itype could be 1 or 2.
    itype == 1: Generate iyr+iday for MPI processing;
    itype == 2: Generate ihr+imin for granule processing, which calls once at the very begining of the Main function.
    """
    times = []
    if itype == 1:
        for iyr in range(2000, 2001):
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
    
    # MPI initialization
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    

    # Get total number of PE
    NUM_CORES = int(sys.argv[1])


    # Main iteration
    times = times_gen(1)[:]
    
    for idd in range(0, len(times), NUM_CORES):
        itime = times[idd+comm_rank]
        
        try:
            main(int(itime[:4]), int(itime[4:]))
        except Exception as err:
            print ">> err: {}".format(err)
            continue
    

    
