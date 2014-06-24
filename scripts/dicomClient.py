import httplib
import urllib, os
from datetime import tzinfo, timedelta, datetime

def getSeriesInstanceUIDs(rootFolder):
  dirs = []
  for path, subdirs, files in os.walk(rootFolder):
    suid = path.split('/')[-1]
    dirs.append(suid)
  return dirs

def getDicomList(sUID):
  c = httplib.HTTPSConnection("mdc.scape.psnc.pl:8183")
  hospital = 'tomek'
  query = '?SeriesInstanceUID=' + sUID
  #query = '?SeriesInstanceUID=1.2.840.113619.2.278.3.168428016.773.1366090221.577'
  c.request("GET", "/dicom/"+hospital+query)
  response = c.getresponse()
  print response.status, response.reason
  data = response.read()
  print data
  return data.split('\n')

def getData(fileList):
  f_name = 'dicomClientTimeLog.txt'
  f = open(f_name,'w')
    
  tSum = timedelta(0)
  for furl in fileList:
    if (furl == ''):
      continue
    fname = furl.split('/')[-1]
    t1 = datetime.now()
    urllib.urlretrieve (furl, "/home/tomhof/dest/"+fname)
    t2 = datetime.now()
    tdelta = t2-t1
    tSum = tSum + tdelta
    f.write(str(tdelta)+'\n')
  f.write('---------------\n')
  f.write(str(tSum)+'\n')
  f.close()
  return 0

def main():
  #hospital = sys.argv[1]
  rootFolder = "/home/tomhof/tmp/dicom/"
  seriesInstanceUIDs = getSeriesInstanceUIDs(rootFolder)
  #print seriesInstanceUIDs
  for sUID in seriesInstanceUIDs[1:4]:
    print '-----' + str(sUID) + '---------\n'
    fileList = getDicomList(sUID)
    getData(fileList)
    print fileList
    print '---------END----------------\n'
    
if __name__=="__main__":main()
