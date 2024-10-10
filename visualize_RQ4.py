import matplotlib.pyplot as plt

# Data
data = {
    "Beer": ["Silver Oak Cab Svgn Alex Vly", 45, 3149.55],
    "Spirits": ["Ketel One Vodka", 843, 25325.57],
    "Wine": ["Simi Chard", 570, 6268.3]
}

# Extracting labels, values, and brand names
categories = list(data.keys())
brand_names = [value[0] for value in data.values()]
sales_quantity = [value[1] for value in data.values()]
total_sales = [value[2] for value in data.values()]

# Plotting total sales quantity
plt.figure(figsize=(10, 5))
bars = plt.bar(categories, sales_quantity, color='skyblue')
plt.xlabel('Category')
plt.ylabel('Total Sales Quantity')
plt.title('Total Sales Quantity of Most Preferred Brand(each Category)')

# Adding brand names above the peak of each bar
for i, bar in enumerate(bars):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1, brand_names[i], ha='center', va='bottom')

plt.show()

# Plotting total sales amount
plt.figure(figsize=(10, 5))
bars = plt.bar(categories, total_sales, color='lightgreen')
plt.xlabel('Category')
plt.ylabel('Total Sales Amount ($)')
plt.title('Total Sales Amount of Most Preferred Brand(each Category)')

# Adding brand names above the peak of each bar
for i, bar in enumerate(bars):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 30, brand_names[i], ha='center', va='bottom')

plt.show()
