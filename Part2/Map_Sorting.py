from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceSorting(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            #date = datetime.split(' ')[0].split('/')[0]
            #status_type = data[1].strip()
            num_reactions = data[3].strip()

            if int(num_reactions) > 3000:
                yield year,num_reactions
            #yield status_type,num_reactions
            
            #if year == '2018' :
            #    if status_type =='video':
            #        yield 'video',data
            #    elif status_type =='photo':
            #        yield 'photo',data
            #    elif status_type =='link':
            #        yield 'link',data
            #    elif status_type =='status':
            #        yield 'status',data
            #    yield date,None
                
    def reducer(self, key, values):

        yield key, sorted(values, reverse=True)
        #lval = []
        #for react in values:
        #    lval.append(react)
        #yield key, lval


    
if (__name__ == '__main__'):
    MapReduceSorting.run()