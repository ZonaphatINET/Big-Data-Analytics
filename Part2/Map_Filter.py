from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReducefilter(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip() # Get the status type
            num_reactions = data[3].strip()
            if year == '2018' and int(num_reactions) > 2000:
                yield status_type,num_reactions
            #if status_type =='video':
            #    yield (year,'video'), 1
            #elif status_type =='photo':
            #    yield (year,'photo'), 1
            #elif status_type =='link':
            #    yield (year,'link'), 1
            #elif status_type =='status':
            #    yield (year,'status'), 1

  
    
if (__name__ == '__main__'):
    MapReducefilter.run()