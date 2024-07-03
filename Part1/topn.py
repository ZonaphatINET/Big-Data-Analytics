from mrjob.job import MRJob
from mrjob.step import MRStep
from heapq import nlargest

class MapReduceAverage(MRJob):

    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip()
            if status_type =='video':
                yield (year,'video'), 1
            elif status_type =='photo':
                yield (year,'photo'), 1
            elif status_type =='link':
                yield (year,'link'), 1
            elif status_type =='status':
                yield (year,'status'), 1

    def reducer_count(self, key, values):
        yield key[0], (sum(values), key[1])

    def reducer_top_n(self, key, values):
        top_n = nlargest(2, values)
        yield key, top_n

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_top_n)
        ]

if __name__ == '__main__':
    MapReduceAverage.run()