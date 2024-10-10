import csv
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from datetime import datetime
import sys

class SalesPatternMRJob(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(SalesPatternMRJob, self).configure_args()
        self.add_passthru_arg('--year', type=int, help='Filter by year', default=datetime.now().year)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def parse_date(self, date_string):
        if '-' in date_string:
            # To handle dates with format dd-mm-yyyy
            date_obj = datetime.strptime(date_string, '%d-%m-%Y')
        elif '/' in date_string:
            # To handle dates with format mm/dd/yyyy
            date_obj = datetime.strptime(date_string, '%m/%d/%Y')
        else:
            # To make all the date columns in one common format
            date_obj = datetime.strptime(date_string, '%d-%m-%Y')

        return date_obj.strftime('%B')

    def mapper(self, _, line):
        fields = ['InventoryId', 'Store', 'Brand', 'Description', 'Size', 'SalesQuantity',
                  'SalesDollars', 'SalesPrice', 'SalesDate', 'Volume', 'Classification',
                  'ExciseTax', 'VendorNo', 'VendorName']
        values = line.split(',')
        data = dict(zip(fields, values))

        try:
            month = self.parse_date(data['SalesDate'])
            category = self.categorize_product(data['Description'])
            quantity = int(data['SalesQuantity'])
            yield f"{category} {month}", f"{quantity}"
        except ValueError:
            pass

    def reducer(self, key, values):
        total_quantity = sum(int(value) for value in values)
        output_row = [key, total_quantity]

        # Append the output to the CSV file
        with open('output.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(output_row)

        # Yield the output for streaming
        yield key, str(total_quantity)

    def categorize_product(self, description):
        description = description.lower()
        if any(word in description for word in ['beer', 'lager', 'ale', 'brew']):
            return 'Beer'
        elif any(word in description for word in ['wine', 'chard', 'merlot', 'cabernet', 'pnt', 'pinot', 'sauvignon']):
            return 'Wine'
        elif any(word in description for word in ['vodka', 'whisky', 'whiskey', 'tequila', 'rum', 'spirits', 'bourbon', 'scotch']):
            return 'Spirits'
        else:
            return 'Other'

if __name__ == '__main__':
    SalesPatternMRJob.run()
