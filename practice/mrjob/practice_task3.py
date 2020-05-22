
from mrjob.job import MRJob
import re

class MRAtLeastOnce(MRJob):
    def mapper(self, _, line):
        entry = line.split(",")
        full_name = (entry[19],entry[20])
        year_match = re.search(r"\d{4}",entry[17])
        
        if year_match is not None:
            year = year_match.group()
            yield full_name, year

    #def combiner(self, full_name, years):
    #    yield full_name, years

    def reducer(self, full_name, years):
        all_years = set(years)

        if len(all_years) == 2:
            #^remember "==" for compare
            yield full_name

if __name__ == "__main__":
    MRAtLeastOnce.run()
