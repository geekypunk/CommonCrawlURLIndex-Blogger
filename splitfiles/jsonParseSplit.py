import json;
from StringIO import StringIO
from gzip import GzipFile
from boto.s3.connection import S3Connection
import shutil
import os
import optparse

parser = optparse.OptionParser()

parser.add_option('-q', '--file',
    action="store", dest="file",
    help="file name", default="spam")

options, args = parser.parse_args()

print 'file name:', options.file



if not os.path.exists('htmlDump'):
    os.makedirs('htmlDump')

with open (options.file, "r") as myfile:
    fileObj=myfile.read();



data = [json.loads(row) for row in fileObj.split("\n") if row]
print len(data)
def arc_file(s3, bucket, info):

    bucket = s3.lookup(bucket)
    keyname = "/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFileParition}.arc.gz".format(**info)
    key = bucket.lookup(keyname)
    
    start = info['arcFileOffset']
    end = start + info['compressedSize'] - 1
    
    headers={'Range' : 'bytes={}-{}'.format(start, end)}
    
    chunk = StringIO(
         key.get_contents_as_string(headers=headers)
    )
    
    return GzipFile(fileobj=chunk).read()

conn = S3Connection('AKIAJ7RCWJDAISNDTA3Q', 'zZyEoH/9yYK0lFcf6/dvcLvzs6+/VzcPghE0uN/1');

error_text_file = open("error.txt", "w+")
log_File = open("log.txt","w+");
i=1;
for u in data:
	log_File.write("Scanning line "+str(u)+"\n")
	try:
		print i
		i = i+1;
		s = arc_file(conn, 'aws-publicdatasets', u)
		profileId = "{url}".format(**u)
		lastIndex = profileId.rfind('/')
		profileId = profileId[lastIndex+1:]
		strprofileId = str(profileId)
		fileName = "htmlDump/"+strprofileId+"_{arcSourceSegmentId}_{arcFileDate}_{arcFileParition}".format(**u)
		text_file = open(fileName, "w+")
		text_file.write(s)
		text_file.close()
		print 'done '+str(i);
	except Exception, err:
		print str(err)
		error_text_file.write(str(err)+"\n")
		pass

error_text_file.close();
log_File.close();




