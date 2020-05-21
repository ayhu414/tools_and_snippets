from mrjob.job import MRJob
import heapq

class MRFreqStaff(MRJob):

    def mapper(self, _, line):
        entry = line.split(",")
        full_name = (entry[19],entry[20])

        yield full_name, 1

    def combiner(self, full_name, count):
        agg = sum(count)

        yield full_name, agg
    
    def reducer_init(self):
        #^BUG INSTANCE, remmeber to declare self
        self.h = [(0,None)]*10
        #^BUG INSTANCE, remember to declare self.XX 
        heapq.heapify(self.h)

    def reducer(self, full_name, agg):
        total = sum(agg)
        min_count = self.h[0][0]
        if total > min_count:
            heapq.heapreplace(self.h, (total, full_name))
        #^BUG INSTANCE, remember to set the total, not the agg val
    def reducer_final(self):
        for ele in self.h:
            yield ele

if __name__ == "__main__":
    MRFreqStaff.run()
