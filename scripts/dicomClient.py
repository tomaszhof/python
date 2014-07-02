import httplib
import urllib, os
from multiprocessing import Pool

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
  #print response.status, response.reason
  data = response.read()
  #print data
  return data.split('\n')

def getData(p): #(fileList):
  from datetime import tzinfo, timedelta, datetime
  (suid, furl) = p
  t1 = datetime.now()
  t2 = t1
  #noFiles = len(fileList)
  tSum = 0
  fSize = 0
  resultsList = []
  #for (suid,furl) in fileList:
  if (furl == ''):
    return []
  fname = furl.split('/')[-1]
  try:
    t1 = datetime.now()
    (n,h) = urllib.urlretrieve (furl, "/home/tomhof/dest/"+fname)
    t2 = datetime.now()
    fSize = h['content-length']
  except IOError:
    #sleep(5)
    t1 = datetime.now()
    (n,h) = urllib.urlretrieve (furl, "/home/tomhof/dest/"+fname)
    t2 = datetime.now()
    fSize = h['content-length'] 
  tdelta = t2-t1
  tdeltaMS = int(tdelta.total_seconds()*1000) #in miliseconds
  tSum = tSum + tdeltaMS
  resultsList.append((suid, fname, fSize, tdeltaMS))
  return resultsList

def writeResultsList(resultList):
  f_name = 'dicomClientTimeLogForFiles.txt'
  f = open(f_name,'a')
  for (suid, fname, fSize, tdeltaMS) in resultList:
    f.write(str(suid) + ';' + fname + ';' + str(fSize) + ';' + str(tdeltaMS) + '\n')
  f.close()
  return 0

def joinResultLists(listOfLists):
  resultList = []
  for l in listOfLists:
    resultList += l
  return resultList

def getDicoms():
  rootFolder = "/home/tomhof/tmp/dicom/"
  seriesInstanceUIDs = getSeriesInstanceUIDs(rootFolder)
  allPairList = []
  for sUID in seriesInstanceUIDs[1:]:
    #print '-----' + str(sUID) + '---------\n'
    fileList = getDicomList(sUID)
    pairList = map(lambda x : (sUID, x), fileList)
    allPairList = allPairList + pairList
    #print '---------END----------------\n'
  return allPairList

def main():
  #hospital = sys.argv[1]
  print 'Get DICOMs list...'
  allPairList = getDicoms()
  print 'done.'
  #prepareStats()
  
  #step (pool size)
  step = 10
  pool = Pool(processes=step)              # start worker processes
 
  #download by splitting
  currBeg = 0
  currEnd = 0
  maxL = len(allPairList)
  while currEnd < maxL :
    currBeg = currEnd
    currEnd = currEnd + step 
    subList = allPairList[currBeg:currEnd]
    resArr = pool.map(getData,subList)
    resultList = joinResultLists(resArr)
    writeResultsList(resultList)
    print 'Downloaded ' + str(currEnd) + '/' + str(maxL) + '\n'
   
  rest = maxL - int(maxL/step)*step;
  subList = allPairList[maxL-rest:maxL]
  resArr = pool.map(getData,subList)
  resultList = joinResultLists(resArr)
  print 'Saving results...'
  writeResultsList(resultList)
  print 'done.' 
  print "Completed." 
    
if __name__=="__main__":main()
