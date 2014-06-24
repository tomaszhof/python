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

def computeSeriesStats(seriesList):
  sumSize = 0
  sumTime = 0
  sumHdfs = 0
  sumHbase = 0
  sumSftp = 0
  for t in seriesList:
    (s,t1,t2,t3,t4) = t
    sumSize = sumSize + s
    sumTime = sumTime + t1
    sumHdfs = sumHdfs + t2
    sumHbase = sumHbase + t3
    sumSftp = sumSftp + t4
  noItems = len(seriesList)
  tmp = (sumSize, sumTime, sumHdfs, sumHbase, sumSftp, noItems)
  print tmp
  return tmp 

def computeStats(res, resFileName):
  resFile = open(resFileName, 'w')
  allSize = 0
  allTime = 0
  allHdfs = 0
  allHbase = 0
  allSftp = 0
  allItemsNo = 0
  allSeriesNo = len(res)
  for k,v in res.items():
    stat = computeSeriesStats(v)
    (sumSize, sumTime, sumHdfs, sumHbase, sumSftp, noItems) = stat
    allSize = allSize+sumSize
    allTime = allTime + sumTime
    allHdfs = allHdfs + sumHdfs
    allHbase = allHbase + sumHbase
    allSftp = allSftp + sumSftp
    allItemsNo = allItemsNo + noItems
  resContent = "All series number: " + str(allSeriesNo) + "\n"
  resContent += "All items number: " + str(allItemsNo) + "\n"
  resContent += "All items size: " + str(allSize) + "\n"
  resContent += "Saving time: " + str(allTime) + "\n"
  resContent += "HDFS saving time: " + str(allHdfs) + "\n"
  resContent += "Hbase saving time: " + str(allHbase) + "\n"
  resContent += "Sftp saving time: " + str(allSftp) + "\n"
  resFile.write(resContent)
  resFile.close()
  
def main():
  inputFileName = sys.argv[1] 
  outputFileName = sys.argv[2]
  res = parseFile(inputFileName)
  computeStats(res, outputFileName)
  print "Prepared result file."

if __name__=="__main__":main()
