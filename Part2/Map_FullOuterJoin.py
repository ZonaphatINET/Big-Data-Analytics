from mrjob.job import MRJob

class MapReduceFullOuterJoin(MRJob):

    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            status_type = data[0]  # Assuming 'FB2' or 'FB3' is the first field
            status_id = data[1]    # Assuming 'status_id' is the second field
            yield status_id, (status_type, data)

    def reducer(self, key, values):
        fb1 = []
        fb2 = []

        for v in values:
            if v[0] == 'FB2':
                fb1.append(v[1])
            elif v[0] == 'FB3':
                fb2.append(v[1])

        if len(fb1) > 0 and len(fb2) > 0:
            for value1 in fb1:
                for value2 in fb2:
                    yield None, value1 + value2
        elif len(fb1) > 0:
            for value1 in fb1:
                yield None, value1
        elif len(fb2) > 0:
            for value2 in fb2:
                yield None, value2

if __name__ == '__main__':
    MapReduceFullOuterJoin.run()
