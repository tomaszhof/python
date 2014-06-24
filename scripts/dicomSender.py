import os
import sys
import shlex, subprocess
from multiprocessing import Process, Pool

def psend(fname):
  command_line = "~/tools/dcm4/dcm4che-2.0.28/bin/dcmsnd DCM@mdc.scape.psnc.pl:7183 " + fname + " -keystore ~/keys/wcpit/wcpit-keystore.jks -keystorepw wcpit. -truststore ~/keys/wcpit/wcpit-truststore.jks -truststorepw wcpit. -tls AES -pdv1"
  args = [command_line]
  print "sendig: " + fname + "[" + str(count) +"]"
  subprocess.Popen(args, shell=True)
  print 'done.'
  return 'Sent: ' + fname

def sendDicomFromFolder(root):
  count = 0
  fnames = []
  p = Pool()
  for path, subdirs, files in os.walk(root):
    for name in files:
        fileToSend = os.path.join(path, name)
	#command_line = "~/tools/dcm4/dcm4che-2.0.28/bin/dcmsnd DCM@localhost:7183 " + fileToSend + " -keystore ~/keys/wcpit/wcpit-keystore.jks -keystorepw wcpit. -truststore ~/keys/wcpit/wcpit-truststore.jks -truststorepw wcpit. -tls AES -pdv1"
	command_line = "~/tools/dcm4/dcm4che-2.0.28/bin/dcmsnd DCM@mdc.scape.psnc.pl:7183 " + fileToSend + " -keystore ~/keys/wcpit/wcpit-keystore.jks -keystorepw wcpit. -truststore ~/keys/wcpit/wcpit-truststore.jks -truststorepw wcpit. -tls AES -pdv1"
	args = [command_line] #shlex.split(command_line)
	#print args
        
	count = count + 1
	if count < 5:
           #fnames.append(fileToSend)
	   print "sendig: " + fileToSend + "[" + str(count) +"]"
	   subprocess.Popen(args, shell=True)
           print "done.\n"
	else:
           #r = p.imap(psend,fnames)
           #del fnames[:]
           #print r.get()
	   return
	#os.system(command_line)        
	#os.system("~/tools/dcm4/dcm4che-2.0.28/bin/dcmsnd DCM@mdc.scape.psnc.pl:7183 " + fileToSend + " -keystore ~/keys/wcpit/wcpit-keystore.jks -keystorepw wcpit. -truststore ~/keys/wcpit/wcpit-truststore.jks -truststorepw wcpit. -tls AES -pdv1")
        #print "done.\n"

def main():
  folderName = sys.argv[1]
  print "Start sending folder " + folderName + " ... \n"
  sendDicomFromFolder(folderName)
  print "Completed."

if __name__=="__main__":main()
