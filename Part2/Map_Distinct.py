from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReducedistinct(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            date = datetime.split(' ')[0].split('/')[0]

            if year == '2018' :
            #if status_type =='video':
            #    yield (year,'video'), 1
            #elif status_type =='photo':
            #    yield (year,'photo'), 1
            #elif status_type =='link':
            #    yield (year,'link'), 1
            #elif status_type =='status':
            #    yield (year,'status'), 1
                yield date,None
                
    def reducer(self, key, values):
        yield key,None


    
if (__name__ == '__main__'):
    MapReducedistinct.run()