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

def getData(p):
  from datetime import tzinfo, timedelta, datetime
  (suid, furl) = p
  t1 = datetime.now()
  t2 = t1
  tSum = 0
  fSize = 0
  resultsList = []
  if (furl == ''):
    return []
  fname = furl.split('/')[-1]
  try:
    t1 = datetime.now()
    (n,h) = urllib.urlretrieve (furl, "/home/tomhof/dest/"+fname)
    t2 = datetime.now()
    fSize = h['content-length']
  except IOError:
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
  f_name = 'dicomClientStats.txt'
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


def getSeriesInstanceUIDsFromCsv():
  f_name = 'seriesInstanceUIDs.csv'
  f = open(f_name,'r')
  suids = f.readlines()
  return suids
  
def getDicoms():
  rootFolder = "/home/tomhof/tmp/dicom/"
  #seriesInstanceUIDs = getSeriesInstanceUIDs(rootFolder)
  #seriesInstanceUIDs = seriesInstanceUIDs[1:]
  seriesInstanceUIDs = getSeriesInstanceUIDsFromCsv()
  
  #f_name = 'seriesInstanceUIDs.csv'
  #f = open(f_name,'w')
  #for suid in seriesInstanceUIDs[1:]:
  #  f.write(suid + '\n')
  #f.close()

  allPairList = []
  for sUID in seriesInstanceUIDs:
    #print '-----' + str(sUID) + '---------\n'
    fileList = getDicomList(sUID)
    pairList = map(lambda x : (sUID, x), fileList)
    pairList = filter(lambda (x,y) : y!='', pairList)
    allPairList = allPairList + pairList
    #print '---------END----------------\n'
  return allPairList

def main():
  from datetime import tzinfo, timedelta, datetime
  print 'Get DICOMs list...'
  t01 = datetime.now()
  allPairList = getDicoms()
  t02 = datetime.now()
  tdelta0 = t02-t01
  tdeltaMS0 = int(tdelta0.total_seconds()*1000) #in miliseconds
  print 'done.'
  #print allPairList
  
  #step (pool size)
  step = 10
  pool = Pool(processes=step)              # start worker processes
 
  #download by splitting
  currBeg = 0
  currEnd = 0
  nextEnd = currEnd + step
  maxL = len(allPairList)
  t1 = datetime.now()
  while nextEnd < maxL :
    currBeg = currEnd
    currEnd = currEnd + step
    nextEnd = currEnd + step
    subList = allPairList[currBeg:currEnd]
    resArr = pool.map(getData,subList)
    resultList = joinResultLists(resArr)
    writeResultsList(resultList)
    print 'Downloaded ' + str(currEnd) + '/' + str(maxL) + '\n'
  t2 = datetime.now()
  tdelta = t2-t1
  tdeltaMS = int(tdelta.total_seconds()*1000) #in miliseconds

  rest = maxL - int(maxL/step)*step;
  currBeg = maxL-rest
  currEnd = maxL
  subList = allPairList[currBeg:currEnd]
  resArr = pool.map(getData,subList)
  resultList = joinResultLists(resArr)
  writeResultsList(resultList)
  print "Completed."

  downLine = 'Downloaded: ' + str(currEnd) + '/' + str(maxL) + '\n'
  totalLine = 'Total time: ' + str(tdeltaMS) + '\n'
  queryLine = 'Query time: ' + str(tdeltaMS0) + '\n'
  threadsLine = 'Threads number: ' + str(step) + '\n'
  f_name = 'dicomClientTime.txt'
  f = open(f_name,'a')
  f.write(downLine)
  f.write(totalLine)
  f.write(queryLine)
  f.write(threadsLine)
  f.close()

  print downLine 
  print totalLine
  print queryLine
  print threadsLine 
    
if __name__=="__main__":main()
