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
    p = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE)
    p.wait()
    out, err = p.communicate()
    status = 1
    msg = 'OK'
    if 'error' in out:
	status = 0
	msg = out
    print 'done. Status [err]: ' + str(err) 
    return (status, msg)

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

def checkStatus(resArr):
  for r in resArr:
    (status, msg) = r
    if (status==0):
	print '----------------[ERROR BEGIN]--------------\n'
        print 'MESSAGE: '+msg
	print '----------------[ERROR END]--------------\n'
	return 0
  return 1

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
  maxL = 20 #len(fList)
  while currEnd < maxL :
    subList = fList[currBeg:currEnd]
    currBeg = currEnd
    currEnd = currEnd + step - 1 
    resArr = pool.map(sendDicom,subList)
    status = checkStatus(resArr)
    print 'Sent ' + str(currEnd) + '/' + str(maxL) + 'files.'
    if (status == 0):
      break

  rest = maxL - int(maxL/5)*5;
  subList = fList[maxL-rest:maxL] 
  print "Completed."

if __name__=="__main__":main()
