from mrjob.job import MRJob

class MRboth(MRJob):
    def mapper(self,_,line):
        entry = line.split(",")
        visitor = (entry[0],entry[1])
        visitee = (entry[19],entry[20])

        yield visitor, "visitor"
        yield visitee, "visitee"

    def reducer(self,name,role):
        all_roles = set(role)

        if len(all_roles) == 2:
            yield name
            #BUG INSTANCE, remember to yield, not print or return

#BUG INSTANCE, remember to write the main:

if __name__ == "__main__":
    MRboth.run()