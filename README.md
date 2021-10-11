# Women Shoe Prices
#### [Dataset Source]( https://www.kaggle.com/datafiniti/womens-shoes-prices )

This dataset contains a list of about 10,000 women shoes, including their brand, price, color, discounts (if available).

The aim was to process the data and perform a few simple queries in order to better understand it and detect any patterns using mainly the following technologies:
- Spark
- Scala
- Scala DataFrames
- SQL

The analysis was perfomed using 2 methods: the first one using **spark's API** and specifically the filter method on the DataFrame to remove any noise from the data, for example "unbranded" shoe categories, or alphabetical shoe sizes.
The next method was performing specific queries on the data using **SQL** to define relationships between different fields.

# Main Queries on Data
- Top 10 Most Expensive Shoe Brands
- Average Original Shoe Price per Brand
- Most Popular Shoe Color in Order (Most Popular to Least Popular
- Most Expensive Shoe Color
- Brands with the Greatest Discounts
- How Shoe Sizes affect its Price


