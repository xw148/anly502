import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":

	infile1 = 's3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv'
	infile2 = 's3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv'

	sc = SparkContext( appName="Geolite")
	lines1 = sc.textFile( infile1 )
	lines2 = sc.textFile( infile2 )

	j1 = lines1.map(lambda line: (line.split(",")[1], line.split(",")[0]))
	j2 = lines2.map(lambda country: (country.split(",")[0],country.split(",")[5]))

	j3 = j1.join(j2).collect()
	
	with open("forensicswiki_bycountry.txt","w") as fout:
	    for (ip,Country) in j3: 
            	fout.write("{}\t{}\n".format(ip,Country))

	sc.stop()
