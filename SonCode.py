from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import heapq
import time
import csv

class MRSONAlgorithm(MRJob):
    def configure_args(self):
        super(MRSONAlgorithm, self).configure_args()
        self.add_passthru_arg('--min-support', type=float, default=0.01, help="Minimum support threshold")
        self.add_passthru_arg('--max-size', type=int, default=3, help="Maximum size of itemsets to consider")
        self.add_passthru_arg('--chunk-size', type=int, default=2500, help="Size of data chunks for processing")
    def steps(self):
        return [
            MRStep(mapper=self.mapper_local_frequent_itemsets,
                   reducer=self.reducer_collect_candidates),
            MRStep(reducer=self.reducer_global_frequent_itemsets)
        ]

    def mapper_local_frequent_itemsets(self, _, line):
        # Considering each line as a list of items
        items = line.strip().split(',')
        items = sorted(set(items))  # Remove duplicates and sort
        # Local Apriori Algorithm: Generate all possible itemsets
        local_counts = {}
        for size in range(1, self.options.max_size + 1):
            for combination in combinations(items, size):
                local_counts[combination] = local_counts.get(combination, 0) + 1
        # Yield only those itemsets that meet local minimum support threshold
        local_min_support = self.options.min_support * len(items)  # Adjust local support threshold
        for itemset, count in local_counts.items():
            if count >= local_min_support:
                yield (itemset, 1)

    def reducer_collect_candidates(self, key, values):
        total = sum(values)
        # Collecting all candidates that could be globally frequent
        yield None, (key, total)

    def reducer_global_frequent_itemsets(self, _, values):
        global_item_counts = {}
        for itemset, count in values:
            global_item_counts[tuple(itemset)] = global_item_counts.get(tuple(itemset), 0) + count
        # Filtering out the global frequent itemsets based on global support
        global_support_threshold = self.options.min_support * 1000  # To record number of transactions
        for itemset, count in global_item_counts.items():
            if count >= global_support_threshold:
                yield 'Global Frequent Itemsets', (itemset, count)

    def reducer_write_to_csv(self, key, values):
        with open('/home/hduser/x22240217/SonOutput.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Itemset', 'Support'])
            for itemset, count in values:
                writer.writerow([','.join(itemset), count])

    def steps(self):
        return [
            MRStep(mapper=self.mapper_local_frequent_itemsets,
                   reducer=self.reducer_collect_candidates),
            MRStep(reducer=self.reducer_global_frequent_itemsets),
            MRStep(reducer=self.reducer_write_to_csv)
        ]

if __name__ == '__main__':
    start_time = time.time()
    MRSONAlgorithm.run()
    end_time = time.time()
    print(f"Processing Time of the program is {end_time - start_time} seconds")
