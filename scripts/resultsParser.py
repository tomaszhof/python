import sys
import os
import string
from collections import defaultdict

def parseFile(f_name):
  f = open(f_name)
  lines = f.readlines()
  f.close()  
  res = defaultdict(list)
  for l in lines:
    dArr = l.split(';')
    key = dArr[0]
    s = int(dArr[1])
    t1 = int(dArr[2])
    t2 = int(dArr[3])
    t3 = int(dArr[4])
    t4 = int(dArr[5].translate(None,'\n'))
    if key in res:
          res[key].append((s,t1,t2,t3,t4))
    else:
          res[key] = [(s,t1,t2,t3,t4)]
  return res

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

def computeSeriesStats(seriesList):
  sumSize = 0
  sumTime = 0
  sumHdfs = 0
  sumHbase = 0
  sumSftp = 0
  minVec = initMinVec(5)
  maxVec = initMaxVec(5)
  for t in seriesList:
    (s,t1,t2,t3,t4) = t
    sumSize = sumSize + s
    sumTime = sumTime + t1
    sumHdfs = sumHdfs + t2
    sumHbase = sumHbase + t3
    sumSftp = sumSftp + t4
    minVec = updateMinVec(t,minVec)
    maxVec = updateMaxVec(t,maxVec)
  noItems = len(seriesList)
  tmp = (sumSize, sumTime, sumHdfs, sumHbase, sumSftp, noItems)
  #print tmp
  return (tmp, minVec,maxVec) 

def computeStats(res, resFileName):
  resFile = open(resFileName, 'w')
  allSize = 0
  allTime = 0
  allHdfs = 0
  allHbase = 0
  allSftp = 0
  allItemsNo = 0
  allSeriesNo = len(res)

  minItemVec = initMinVec(5)
  maxItemVec = initMaxVec(5)
  minSeriesVec = initMinVec(6)
  maxSeriesVec = initMaxVec(6)
  for k,v in res.items():
    stat = computeSeriesStats(v)
    (sumSize, sumTime, sumHdfs, sumHbase, sumSftp, noItems) = stat[0]
    allSize = allSize+sumSize
    allTime = allTime + sumTime
    allHdfs = allHdfs + sumHdfs
    allHbase = allHbase + sumHbase
    allSftp = allSftp + sumSftp
    allItemsNo = allItemsNo + noItems
    minSeriesVec = updateMinVec(stat[0],minSeriesVec)
    maxSeriesVec = updateMaxVec(stat[0],maxSeriesVec)
    minItemVec = updateMinVec(stat[1],minItemVec)
    maxItemVec = updateMaxVec(stat[2],maxItemVec)
  resContent = "All series number: " + str(allSeriesNo) + "\n"
  resContent += "All items number: " + str(allItemsNo) + "\n"
  resContent += "All items size: " + str(allSize) + "\n"
  resContent += "Saving time: " + str(allTime) + "\n"
  resContent += "HDFS saving time: " + str(allHdfs) + "\n"
  resContent += "Hbase saving time: " + str(allHbase) + "\n"
  resContent += "Sftp saving time: " + str(allSftp) + "\n\n"
  
  #seriesStats
  resContent += "Maximum series stats \n"
  resContent += "Max series size: " + str(maxSeriesVec[0]) + "\n"
  resContent += "Max series sending time: " + str(maxSeriesVec[1]) + "\n"
  resContent += "Max series hdfs saving time: " + str(maxSeriesVec[2]) + "\n"
  resContent += "Max series hbase saving time: " + str(maxSeriesVec[3]) + "\n"
  resContent += "Max series sftp saving time: " + str(maxSeriesVec[4]) + "\n"
  resContent += "Max series items number: " + str(maxSeriesVec[5]) + "\n"

  resContent += "Minimum series stats \n"
  resContent += "Min series size: " + str(minSeriesVec[0]) + "\n"
  resContent += "Min series sending time: " + str(minSeriesVec[1]) + "\n"
  resContent += "Min series hdfs saving time: " + str(minSeriesVec[2]) + "\n"
  resContent += "Min series hbase saving time: " + str(minSeriesVec[3]) + "\n"
  resContent += "Min series sftp saving time: " + str(minSeriesVec[4]) + "\n"
  resContent += "Min series items number: " + str(minSeriesVec[5]) + "\n\n"
  

  #itemsStats
  resContent += "Maximum items stats \n"
  resContent += "Max item size: " + str(maxItemVec[0]) + "\n"
  resContent += "Max item sending time: " + str(maxItemVec[1]) + "\n"
  resContent += "Max item hdfs saving time: " + str(maxItemVec[2]) + "\n"
  resContent += "Max item hbase saving time: " + str(maxItemVec[3]) + "\n"
  resContent += "Max item sftp saving time: " + str(maxItemVec[4]) + "\n"

  resContent += "Minimum items stats \n"
  resContent += "Min item size: " + str(minItemVec[0]) + "\n"
  resContent += "Min item sending time: " + str(minItemVec[1]) + "\n"
  resContent += "Min item hdfs saving time: " + str(minItemVec[2]) + "\n"
  resContent += "Min item hbase saving time: " + str(minItemVec[3]) + "\n"
  resContent += "Min item sftp saving time: " + str(minItemVec[4]) + "\n\n"
 

  #summary
  allTimeInSec = allTime/1000
  objPerSec = float(allItemsNo) / allTimeInSec
  throughputInBytesPerSec = float(allSize) / allTimeInSec
  maxObjSizeInBytes = maxItemVec[0]
  minObjSizeInBytes = minItemVec[0]
  
  resContent += "Number of objects per second: " + str(objPerSec) + " [obj/s]\n"
  resContent += "Throughput in bytes per second: " + str(throughputInBytesPerSec) + " [bytes/s]\n"
  resContent += "Max object size handled in bytes: " + str(maxObjSizeInBytes) + " [bytes]\n"
  resContent += "Min object size handled in bytes: " + str(minObjSizeInBytes) + " [bytes]\n"

  resFile.write(resContent)
  resFile.close()
  
def main():
  inputFileName = sys.argv[1] 
  outputFileName = sys.argv[2]
  res = parseFile(inputFileName)
  computeStats(res, outputFileName)
  print "Prepared result file."

if __name__=="__main__":main()
