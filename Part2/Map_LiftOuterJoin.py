from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceLiftOuterJoin(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') 
            
            id = data[1]

            yield id, data



    def reducer(self, key, values):
        fb1 = []
        fb2 = []
        for v in values:
            if v[0] == 'FB2':
                fb1.append(v)
            elif v[0] == 'FB3':
                fb2.append(v)

        for i in fb1:
            for j in fb2:
                yield None, (i+j)

       
   
if (__name__ == '__main__'):
    MapReduceLiftOuterJoin.run()