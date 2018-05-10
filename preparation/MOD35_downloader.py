
# from ftplib import FTP
# from my_module import np, os, sys
# import urllib
# import csv
# import mpi4py.MPI as MPI


# #
# def download_date(year, day):

#     # Login MODIS FTP and change to the specified directory
#     ftp = FTP('ladsweb.nascom.nasa.gov')
#     ftp.login('anonymous', passwd='smyiz@gmail.com')

#     # data folder
#     idate = '{}/{}'.format(iyr, str(day).zfill(3))
#     ifolder_server = os.path.join('/allData/61/MOD35_L2', idate)

#     # catch errors of bad days (e.g., June 31)
#     try:
#         ftp.cwd(ifolder_server)
#     except Exception as err:
#         print ">> cwd err: {} on {}".format(err, ifolder_server)
#         return

    
#     # generate download list
#     files = ftp.nlst()
#     hdf_files = np.array([i for i in files if i.endswith('hdf')])

#     if len(hdf_files) == 0:
#         return


#     # mkdir destination folder on BWs, continue if it is already existed
#     ifolder_local = "/u/sciteam/smzyz/scratch/data/MODIS/MOD35/{}".format(idate)
    
#     try:
#         os.mkdir(ifolder_local)
#     except:
#         #print ">> mkdir err: Folder already exists, continue"
#         os.chdir(ifolder_local)


#     # download MOD35 files to the destination folder, continue if it is already existed
#     exist_files = os.listdir(ifolder_local)
    
#     for ifile in hdf_files:
#         if ifile in exist_files:
#             continue
        
#         try:
#             with open("{}/{}".format(ifolder_local, ifile), 'wb') as f:
#                 ftp.retrbinary('RETR {}'.format(ifile), f.write)
#         except Exception as err:
#             print ">> download err: {}, program terminated".format(err)
#             return 



# def download_date_v2(iyr, iday):
#     dst_parent_folder = "/u/sciteam/smzyz/scratch/data/MODIS/MOD35"
#     src_parent_folder = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD35_L2'
    
    
#     # 1) Download .csv file containing all available data files
#     src_csv_file = "{}/{}/{}.csv".format(src_parent_folder, iyr, str(iday).zfill(3))
#     dst_csv_file = "{}/{}.csv".format(dst_parent_folder, str(iday).zfill(3))
        
#     print src_csv_file, dst_csv_file
#     urllib.urlretrieve(src_csv_file, dst_csv_file)
    
    
#     # 2) Read csv file and generate a download file list
#     files = [ f['name'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
#     src_file_list = ["{}/{}/{}/{}".format(src_parent_folder, iyr, str(iday).zfill(3), f) for f in files]
#     dst_file_list = ["{}/{}".format(dst_parent_folder, f) for f in files]
    

#     # 3) Download
#     for i in tqdm(range(len(src_file_list))):
#         isrc = src_file_list[i]
#         idst = dst_file_list[i]

#         urllib.urlretrieve(isrc, idst)
            





from my_module import np, os, tqdm, sys
import csv
import shutil
import ssl
import urllib2
import mpi4py.MPI as MPI



USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')


def url_retrieve(src, dst):
    url = src
    headers = { 'user-agent' : USERAGENT }
    headers['Authorization'] = 'Bearer ' + "59451810-53C9-11E8-9F28-C61EAE849760"
    CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    
    try:
        fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
        shutil.copyfileobj(fh, dst)
    except urllib2.HTTPError as e:
        print 'HTTP GET error code: %d' % e.code()
        print 'HTTP GET error message: %s' % e.message
    except urllib2.URLError as e:
        print 'Failed to make request: %s' % e.reason



def main(iyr, iday):
    iday = str(iday).zfill(3)
    
    dst_parent_folder = "/u/sciteam/smzyz/scratch/data/MODIS/MOD35"
    try:
        os.mkdir("{}/{}/{}".format(dst_parent_folder, iyr, iday))
        dst_parent_folder = "{}/{}/{}".format(dst_parent_folder, iyr, iday)
    except OSError as err:
        dst_parent_folder = "{}/{}/{}".format(dst_parent_folder, iyr, iday)
        return 
    
    src_parent_folder = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD35_L2'
    
    
    # 1) Download .csv file containing all available data files
    src_csv_file = "{}/{}/{}.csv".format(src_parent_folder, iyr, iday)
    dst_csv_file = "{}/{}.csv".format(dst_parent_folder, iday)
    with open(dst_csv_file, 'w+b') as out:
        url_retrieve(src_csv_file, out)
    
    
    # 2) Read csv file and generate a download file list
    files = [ f['name'] for f in csv.DictReader(open(dst_csv_file), skipinitialspace=True) ]
    src_file_list = ["{}/{}/{}/{}".format(src_parent_folder, iyr, iday, f) for f in files]
    dst_file_list = ["{}/{}".format(dst_parent_folder, f) for f in files]
    
    
    # 3) Download
    for i in range(len(src_file_list)):
        isrc = src_file_list[i]
        idst = dst_file_list[i]
        try:
            with open(idst, 'w+b') as out:
                url_retrieve(isrc, out)
        except Exception as err:
            #print ">> err: {}".format(err)
            continue
            
         
        
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
        print ">> log: PE {} process {}".format(comm_rank, itime)

        try:
            main(int(itime[:4]), int(itime[4:]))
        except Exception as err:
            #print ">> err: {}".format(err)
            continue
    
    
    
    
