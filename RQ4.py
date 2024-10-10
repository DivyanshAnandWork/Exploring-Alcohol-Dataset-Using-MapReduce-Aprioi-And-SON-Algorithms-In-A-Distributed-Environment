from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTopSellingProducts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sales,
                   reducer=self.reducer_sum_sales),
            MRStep(reducer=self.reducer_find_max_sales)
        ]

    def categorize_product(self, description):
        description = description.lower()
        if any(word in description for word in ['beer', 'lager', 'ale', 'brew']):
            return 'Beer'
        elif any(word in description for word in ['wine', 'chard', 'merlot', 'cabernet', 'pinot', 'sauvignon']):
            return 'Wine'
        elif any(word in description for word in ['vodka', 'whisky', 'whiskey', 'tequila', 'rum', 'spirits', 'bourbon', 'scotch']):
            return 'Spirits'
        else:
            return 'Other'

    def mapper_get_sales(self, _, line):
        # Splitting the input file, to get the required columns
        line = line.split(',')
        try:
            store = line[1].strip()
            brand = line[2].strip()
            description = line[3].strip()
            sales_quantity = int(line[5].strip())
            sales_dollars = float(line[6].strip())
            category = self.categorize_product(description)
            yield ((store, category, brand, description), (sales_quantity, sales_dollars))
        except:
            pass  # Skip lines where data cannot be properly parsed

    def reducer_sum_sales(self, key, values):
        total_quantity = 0
        total_dollars = 0
        for count, amount in values:
            total_quantity += count
            total_dollars += amount
        yield (key[1], (key[0], key[2], key[3], total_quantity, total_dollars))

    def reducer_find_max_sales(self, category, values):
        # Finding the product with the maximum sales dollars in each category
        max_dollars = 0
        max_product = None
        for store, brand, description, quantity, dollars in values:
            if dollars > max_dollars:
                max_dollars = dollars
                max_product = (store, brand, description, quantity, dollars)
        if category != 'Other':  # We only want to output Beer, Wine, or Spirits
            yield (category, max_product)

if __name__ == '__main__':
    MRTopSellingProducts.run()
