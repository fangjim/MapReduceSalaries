from mrjob.job import MRJob
import heapq  # for maintaining a min heap of top N elements

class Top10MapMR(MRJob):

    def __init__(self, *args, **kwargs):
        super(Top10MapMR, self).__init__(*args, **kwargs)
        self.top10 = []  # Initialize a list to keep track of top 10 salaries

    def mapper_init(self):
        self.top10 = []  # Re-initialize for each mapper

    def mapper(self, _, line):
        salary = int(line)  # Assuming each line in the file is a salary figure
        heapq.heappush(self.top10, salary)  # Add salary to heap
        if len(self.top10) > 10:  # If more than 10 salaries, remove the smallest
            heapq.heappop(self.top10)

    def mapper_final(self):
        for salary in self.top10:
            yield None, salary  # Emit all top 10 salaries from this mapper

    def reducer(self, _, salaries):
        final_top10 = []
        for salary in salaries:
            heapq.heappush(final_top10, salary)
            if len(final_top10) > 10:
                heapq.heappop(final_top10)

        for salary in final_top10:
            yield salary, None

if __name__ == '__main__':
    Top10MapMR.run()


# from mrjob.job import MRJob
# import heapq

# class Top10MapMR(MRJob):

#     def mapper(self, _, line):
#         salary = int(line)
#         yield "top_salaries", salary

#     def reducer(self, key, values):
#         top10 = heapq.nlargest(10, values)
#         for salary in top10:
#             yield key, salary

# if __name__ == '__main__':
#     Top10MapMR.run()