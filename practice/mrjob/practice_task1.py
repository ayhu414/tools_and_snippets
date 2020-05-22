from mrjob.job import MRJob
import re

class MRTenTimes(MRJob):

    def mapper(self, _, line):
        entry = line.split(",")
        full_name = (entry[0],entry[1])
        #^ INSTANCE, note to use entry, NOT line
        yield full_name, 1

    def combiner(self, full_name, count):
        total = sum(count)

        yield full_name, total
    
    def reducer(self, full_name, total):
        final = sum(total)

        if final>=10:
        #^ INSTANCE, note to check conditions ">="
            yield full_name, final

if __name__ == "__main__":
    MRTenTimes.run()