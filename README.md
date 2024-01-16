![header_logo](https://drive.google.com/uc?export=view&id=1BWk6rVHO8R8PcNrbat7ETdJBQsORSJ3l)
## eCommerce Retail Data Science pipeline
#### Data service for retailers and manufacturers
> *As of Jan. 2024, collection from ecommerce site is halted due to the need fo API Authorization and tightened security measures on their end.*

#### Summary
----

eCommerce Retail is a passion project for collection of goods / products online specifically on food & beverage.
The stages of the end to end pipeline are as follows:
1. Collection to an eCommerce site specifically `shopee.ph` under their "SuperMarket Shop". The collection consists of different categories from


| category_name | item_code |
| --- | --- |
| Breakfast Food | 11021235 |
| Medical Supplies | 11021330 |
| Hair Care | 11021276 |
| Skin Care | 11021263 |
| Bath and Body | 11021285 |
| Beverages | 11021200 |
| Laundry & Household | 11021207 |
| Pet Care | 110218881 |
| Personal Care | 11021293 |
| Snacks and Sweets | 11021224 |
| Seasoning Staple Foods & Baking Ingredients | 11034582 |
| Babies and Kids | 11021766 |

2. SMTP Notifaction System for Data Collection Reporting [Preview](https://drive.google.com/uc?export=view&id=1S-WmYmDo0rTyB7xUuI0x4FoRtKC12Gq-)

3. Data Wrangling and Transormation. Majority of the transformation load was done through `pandas-flavor`:

```
self.transformed_df = self.input_frame \
    .tr_pipe.convert_id_type() \
    .tr_pipe.cl_product_brand() \
    .tr_pipe.cl_product_price() \
    .tr_pipe.cl_product_rating() \
    .tr_pipe.create_pack_size_desc() \
    .tr_pipe.create_median_price() \
    .tr_pipe.create_pages() \
    .tr_pipe.create_cat_names() \
    .tr_pipe.create_pack_names()\
    .tr_pipe.fix_fill_names()
```

Using dot notation makes the transformation pipeline easier to document and readable.

4. Loading the clean dataset into tableau dashboard. [Pepsi Dashboard](https://public.tableau.com/app/profile/jae.cabrera/viz/PepsiDashboard/MarketSales?publish=yes)
5. And lastly created a machine learning model to predict prices.  
