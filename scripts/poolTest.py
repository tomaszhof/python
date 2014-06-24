import os
import sys
import subprocess

from multiprocessing import Pool

def f(x):
    return x*x

def sendDicom(fname):
    cmdBegin = "~/tools/dcm4/dcm4che-2.0.28/bin/dcmsnd DCM@mdc.scape.psnc.pl:7183 "
    cmdEnd = " -keystore ~/keys/wcpit/wcpit-keystore.jks -keystorepw wcpit. -truststore ~/keys/wcpit/wcpit-truststore.jks -truststorepw wcpit. -tls AES -pdv1 -tcpdelay -releaseTO 10000"
    command_line = cmdBegin + fname + cmdEnd
    args = [command_line]
    print 'Sending dicom file: ' + fname
    p = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE).wait()
    out, err = p.communicate()
    print 'done.'
    return 1

def getDicomList(root):
  #count = 0
  fnames = []
  p = Pool()
  for path, subdirs, files in os.walk(root):
    for name in files:
        fileToSend = os.path.join(path, name)
        fnames.append(fileToSend)
        #count = count + 1
        #if (count ==5):
        #   return fnames
  return fnames

def main():
  #step (pool size)
  step = 5
  pool = Pool(processes=step)              # start worker processes
  rootFolderName = sys.argv[1]
  print "Start sending folder " + rootFolderName + " ... \n"
  
  #get list of files
  fList = getDicomList(rootFolderName)
  
  #send by splitting
  currBeg = 0
  currEnd = 0
  maxL = len(fList)
  while currEnd < maxL :
    subList = fList[currBeg:currEnd]
    currBeg = currEnd
    currEnd = currEnd + step - 1 
    print pool.map(sendDicom,subList)
    print 'Sent ' + str(currEnd + 1) + '/' + str(maxL) + 'files.'

  rest = maxL - int(maxL/5)*5;
  subList = fList[maxL-rest:maxL] 
  print "Completed."

if __name__=="__main__":main()
