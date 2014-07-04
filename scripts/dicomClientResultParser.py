import os, sys

def initMinVec(vLen):
  minVec = [0]*vLen
  for i in range(vLen):
    minVec[i] = sys.maxint
  return minVec

def initMaxVec(vLen):
  maxVec = [0]*vLen
  for i in range(vLen):
    maxVec[i] = -sys.maxint-1
  return maxVec


def updateMinVec(currT, minT):
   if (len(currT)==len(minT)):
	indVec = range(len(minT))
	for i in indVec :
	   if currT[i] < minT[i]:
		minT[i] = currT[i]
   return minT

def updateMaxVec(currT, maxT):
   if (len(currT)==len(maxT)):
	indVec = range(len(maxT))
	for i in indVec :
	   if currT[i] > maxT[i]:
		maxT[i] = currT[i]
   return maxT

def prepareStats(fLogName, tLogName, rFileName):
  logFilesName = fLogName
  f = open(logFilesName,'r')
  fLines = f.readlines()
  f.close()
  logTimeName = tLogName
  f1 = open(logTimeName,'r')
  tLines = f1.readlines()
  f1.close()
  
  minStatsVec = initMinVec(2)
  maxStatsVec = initMaxVec(2)
  tSum = 0
  sizeSum = 0
  noFiles = len(fLines)
  sResults = dict()
  for fl in fLines:
    ftime = int(fl.split(';')[3])
    fsize = int(fl.split(';')[2])
    suid  = fl.split(';')[0]
    if suid not in sResults:
      sResults[suid] = (ftime, fsize)
    else:
      (currTime, currSize) = sResults[suid]
      sResults[suid] = (currTime + ftime, currSize + fsize)
    tSum = tSum + ftime
    sizeSum = sizeSum + fsize
    minStatsVec = updateMinVec(minStatsVec, [ftime,fsize])
    maxStatsVec = updateMaxVec(maxStatsVec, [ftime,fsize])

  resultContent = '--- STATS FOR DICOM FILES --- \n'
  resultContent += 'All files downloading time: ' + str(tSum) + ' [ms] \n'
  resultContent += 'All files number: ' + str(noFiles) + ' \n'
  resultContent += 'All files size: ' + str(sizeSum) + ' [bytes] \n'
  resultContent += 'Avg file downloading time: ' + str(int(tSum/noFiles)) + ' [ms] \n'
  resultContent += 'Downloading speed - files per second: ' + str(float(noFiles)/(tSum/1000)) + ' [obj/s] \n'
  resultContent += 'Downloading speed - bytes per second: ' + str(float(sizeSum)/(tSum/1000)) + ' [bytes/s] \n'
  resultContent += 'Minimum file size : ' + str(minStatsVec[1]) + ' [bytes] \n'
  resultContent += 'Minimum file downloading time : ' + str(minStatsVec[0]) + ' [ms] \n'
  resultContent += 'Maximum file size : ' + str(maxStatsVec[1]) + ' [bytes] \n'
  resultContent += 'Maximum file downloading time : ' + str(maxStatsVec[0]) + ' [ms] \n\n'
  
  
  tSum = 0
  minStatsVec = initMinVec(2)
  maxStatsVec = initMaxVec(2)
  noSeries = len(sResults)
  for (sTime, sSize) in sResults.values():
    minStatsVec = updateMinVec(minStatsVec, [sTime,sSize])
    maxStatsVec = updateMaxVec(maxStatsVec, [sTime,sSize])
    tSum = tSum + sTime

  joinedDownloadTime = tLines[1].split(':')[1].translate(None, '\n')
  joinedQueryTime = tLines[2].split(':')[1].translate(None, '\n')
  threadsNumber = tLines[3].split(':')[1].translate(None, '\n')

  resultContent += '--- STATS FOR DICOM SERIES --- \n'
  resultContent += 'All series downloading time: ' + str(tSum) + ' [ms] \n'
  resultContent += 'All series number: ' + str(noSeries) + ' \n'
  resultContent += 'Avg series downloading time: ' + str(float(tSum)/noSeries) + ' [ms] \n'
  resultContent += 'Downloading speed - files per second: ' + str(float(noSeries)/(tSum/1000)) + ' [series/s] \n'
  resultContent += 'Minimum series size : ' + str(minStatsVec[1]) + ' [bytes] \n'
  resultContent += 'Minimum series downloading time : ' + str(minStatsVec[0]) + ' [ms] \n'
  resultContent += 'Maximum series size : ' + str(maxStatsVec[1]) + ' [bytes] \n'
  resultContent += 'Maximum series downloading time : ' + str(maxStatsVec[0]) + ' [ms] \n\n'

  resultContent += '--- PARALLEL STATISTICS --- \n'
  resultContent += 'Downloading time (parallel): ' + joinedDownloadTime + ' [ms] \n'
  resultContent += 'Quering time : ' + joinedQueryTime + ' [ms] \n'
  resultContent += 'Threads number : ' + threadsNumber + ' \n'
  resultContent += 'Downloading speed (parallel) - files per second: ' + str(float(noFiles)/(int(joinedDownloadTime)/1000)) + ' [obj/s] \n'
  resultContent += 'Downloading speed (parallel) - series per second: ' + str(float(noSeries)/(int(joinedDownloadTime)/1000)) + ' [obj/s] \n'
  resultContent += 'Downloading speed (parallel) - bytes per second: ' + str(float(sizeSum)/(int(joinedDownloadTime)/1000)) + ' [bytes/s] \n'
  
  resultsFileName = rFileName
  r = open(resultsFileName,'w')
  r.write(resultContent)
  r.close()

def main():
  fileLogName = sys.argv[1]
  timeLogName = sys.argv[2]
  resultFileName = sys.argv[3]
  prepareStats(fileLogName, timeLogName, resultFileName)  
  
if __name__=="__main__":main()

