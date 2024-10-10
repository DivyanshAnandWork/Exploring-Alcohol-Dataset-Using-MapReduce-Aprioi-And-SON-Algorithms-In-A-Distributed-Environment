from mrjob.job import MRJob
from mrjob.step import MRStep

class SalesCorrelationMRJob(MRJob):

    def mapper(self, _, line):
        data = line.split(',')
        try:
            # Taking index based on your input file 
            brand = int(data[2])  # Brand index
            excise_tax = float(data[11])  # ExciseTax index
            sales_dollars = float(data[6])  # SalesDollars index
            sales_quantity = int(data[5])  # SalesQuantity index

            # Output each pair combination of columns
            columns = [sales_dollars, sales_quantity, brand, excise_tax]
            col_count = len(columns)
            for i in range(col_count):
                for j in range(i, col_count):  # range(i, col_count) used to avoid duplicate pairs
                    yield f"{i}-{j}", (columns[i], columns[j])
        except ValueError:
            # Skip lines with conversion errors
            pass

    def combiner(self, key, values):
        x = y = xsq = ysq = xy = n = 0.0
        for value in values:
            x += value[0]
            y += value[1]
            xsq += value[0]**2
            ysq += value[1]**2
            xy += value[0] * value[1]
            n += 1
        yield key, (x, y, xsq, ysq, xy, n)

    def reducer(self, key, values):
        x = y = xsq = ysq = xy = n = 0.0
        for value in values:
            x += value[0]
            y += value[1]
            xsq += value[2]
            ysq += value[3]
            xy += value[4]
            n += value[5]

        # To calculate Pearson correlation coefficient
        numerator = xy - ((x * y) / n)
        denominator_l = xsq - (x ** 2) / n
        denominator_r = ysq - (y ** 2) / n
        if denominator_l * denominator_r > 0:
            correlation = numerator / (denominator_l * denominator_r) ** 0.5
            yield key, correlation
        else:
            yield key, None

if __name__ == '__main__':
    SalesCorrelationMRJob.run()
