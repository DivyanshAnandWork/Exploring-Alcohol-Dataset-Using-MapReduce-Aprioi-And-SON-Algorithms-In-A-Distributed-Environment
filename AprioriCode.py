from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import heapq
import time
import csv

class MRAprioriOptimized(MRJob):
    def configure_args(self):
        super(MRAprioriOptimized, self).configure_args()
        self.add_passthru_arg('--min-support', type=float, default=0.01, help="Minimum support  value")
        self.add_passthru_arg('--max-size', type=int, default=3, help="Maximum size of itemsets")

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_itemsets,
                   combiner=self.combiner_count_itemsets,
                   reducer=self.reducer_count_itemsets),
            MRStep(reducer=self.reducer_find_top_combinations)
        ]

    def mapper_get_itemsets(self, _, line):
        items = line.strip().split(',')
        items = sorted(set(items))  # Removing and Sorting Duplicates
        for size in range(1, min(self.options.max_size + 1, len(items) + 1)):
            for combination in combinations(items, size):
                yield (combination, 1)

    def combiner_count_itemsets(self, key, values):
        yield (key, sum(values))

    def reducer_count_itemsets(self, key, values):
        total = sum(values)
        if total >= self.options.min_support * 1000:  # 1000- Total number of transactions
            yield None, (total, key)

    def reducer_find_top_combinations(self, _, values):
        with open('/home/hduser/x22240217/aprioriOutput.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Itemset', 'Support'])  # For the header row
            for value in values:
                csvwriter.writerow([",".join(value[1]), value[0]])  # Writing Itemsets and Support Values to CSV
               

if __name__ == '__main__':
    start_time = time.time()
    MRAprioriOptimized.run()
    end_time = time.time()
    print(f"Runtime of the program is {end_time - start_time} seconds")
