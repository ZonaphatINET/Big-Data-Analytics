from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceBinning(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            status_type = data[1].strip()
            #date = datetime.split(' ')[0].split('/')[0]

            if year == '2018' :
                if status_type =='video':
                    yield 'video',data
                elif status_type =='photo':
                    yield 'photo',data
                elif status_type =='link':
                    yield 'link',data
                elif status_type =='status':
                    yield 'status',data
            #    yield date,None
                
    #def reducer(self, key, values):
    #    yield key,None


    
if (__name__ == '__main__'):
    MapReduceBinning.run()