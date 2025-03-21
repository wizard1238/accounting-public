import copy
import math
import concurrent.futures
from enum import Enum
import pandas as pd

class ProductTypes(Enum):
    """"Enum of possible product types"""
    BUNDLE = "Bundle"
    JUPITER_APPLICATION_PACKAGES = "Jupiter Application Packages"
    JUPITER_PLATFORM_PACKAGES = "Jupiter Platform Packages"
    JUPITER_SERVICES_CS_PACKAGES = "Jupiter Services/CS Packages"
    JUPITER_USERS = "Jupiter Users"
    PRODUCT = "Product"
    SERVICE = "Service"
    SERVICES = "Services"
    SUPPORT = "Support"
    USERS = "Users"
    DIGITAL_HUB_CUSTOMER_EDUCATION = "Digital Hub: Customer Education"
    INSIDED = "Insided"
    CUSTOMER_SUCCESS = "Customer Success"
    PRODUCT_EXPERIENCE = "Product Experience"
    CUSTOMER_EDUCATION = "Customer Education"
    CUSTOMER_COMMUNITIES = "Customer Communities"
    STAIRCASE_AI = "Staircase AI"


# for making sure case is consistent
PRODUCT_TYPE_CASE_LUT = {}
for product in ProductTypes:
    PRODUCT_TYPE_CASE_LUT.update({
        product.value.lower(): product.value
    })


def median(input):
    """find median, given input = {data: [<rows from df>]}"""
    df = pd.DataFrame(data=input["data"])
    return df["Unit Selling Price"].median()


def compliance(input):
    """find compliance for medians +- 25%, for median values +- 50%, given input = {data: [<rows from df>], median: x}"""
    starting_median = input["median"]
    df = pd.DataFrame(data=input["data"])
    length = len(input["data"])
    medians = []

    if length == 0 : return medians

    interval = starting_median / 100
    for i in range(-100, 101):
        median = starting_median + i * interval
        
        upper_bound = median + median * 0.25
        lower_bound = median - median * 0.25

        count = df.query(f"`Unit Selling Price` >= {lower_bound} & `Unit Selling Price` <= {upper_bound}").shape[0]

        medians.append({
            "median": starting_median,
            "calculated SSP": median,
            "low": lower_bound,
            "high": upper_bound,
            "in range": count,
            "total": length,
            "percentage:": i,
            "compliance": count / length,
        })
    
    return medians


def total_ext_selling_price(input):
    """find total ext selling price, given input = {data: [<rows from df>]}"""
    df = pd.DataFrame(data=input["data"])
    return df["Ext Selling Price"].sum()


def to_dataframe(products):
    """convert input to dataframe to write to excel"""
    df = pd.DataFrame()

    for product in products:
        for currency in products[product]:
            for market in products[product][currency]:
                product_info = {
                    "trans currency": currency,
                    "customer class": market,
                    "product SKU": product,
                    "total ext selling price":  products[product][currency][market]["total_ext_selling_price"],
                }
                
                medians = sorted(products[product][currency][market]["medians"], key=lambda e: e["compliance"], reverse=True)
                if (len(medians) > 0):
                    df = pd.concat([df, pd.DataFrame([{**product_info, **products[product][currency][market]["medians"][100]}])], ignore_index=True) # always include median
                    for i in range(10):
                        
                        df = pd.concat([df, pd.DataFrame([{**product_info, **medians[i]}])], ignore_index=True)

    return df


def write_to_excel(products: list[pd.DataFrame], output_path: str):
    """write products to excel"""
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    to_dataframe(products[0]).to_excel(writer, sheet_name='results')
    pd.DataFrame(data=products[1]).to_excel(writer, sheet_name='reconciliation')
    writer.close()


def products_template():
    """create an empty dictionary to store product data"""
    data_template = {
        "data": [],
        "median": 0,
        "medians": [],
        "total_ext_selling_price": 0,
    }

    currency_template = {
        "GENERAL MARKETS": copy.deepcopy(data_template),
        "ENTERPRISE": copy.deepcopy(data_template)
    }

    return {
        "EUR": copy.deepcopy(currency_template),
        "GBP": copy.deepcopy(currency_template),
        "USD": copy.deepcopy(currency_template),
    }


def populate_product_details(market):
    """pylint being dumb"""
    market["median"] = median(market)
    market["medians"] = compliance(market)
    market["total_ext_selling_price"] = total_ext_selling_price(market)


def process_data(filepath, output_path, finished_callback, progress_callback=None):
    """Process the data and return a dataframe"""

    df = pd.read_excel(filepath, sheet_name="input")

    counts = {}
    products = {}

    for product in ProductTypes:
        counts.update({
            product.value: {
                "positive": {
                    "price": 0, # "Ext Selling Price"
                    "quantity": 0, # "Order Quantity"
                },
                "negative": {
                    "price": 0,
                    "quantity": 0
                }
            }
        })


    for row in reversed(range(len(df.index))):
        family = df.loc[row]["Product Family"]
        order_quantity = 0 if math.isnan(df.loc[row]["Order Quantity"]) else float(df.loc[row]["Order Quantity"])
        order_direction = "positive" if order_quantity > 0 else "negative"
        unit_selling_price = 0 if math.isnan(df.loc[row]["Unit Selling Price"]) else float(df.loc[row]["Unit Selling Price"])
        ext_selling_price = 0 if math.isnan(df.loc[row]["Ext Selling Price"]) else df.loc[row]["Ext Selling Price"]

        # remove casing differences from family
        try:
            family = PRODUCT_TYPE_CASE_LUT.get(family.lower(), family)
        except:
            raise ValueError(f"Invalid product family: {family}")

        counts[family][order_direction]["price"] += ext_selling_price
        counts[family][order_direction]["quantity"] += 1

        match family:
            case ProductTypes.SUPPORT.value:
                df.drop([row])
                continue
            case _:
                if order_quantity <= 0 or unit_selling_price <= 0:
                    df.drop([row])
                    continue

        trans_currency = df.loc[row]["Trans Currency"]
        customer_class = df.loc[row]["Customer Class"]
        product_sku = df.loc[row]["Product SKU"]

        if pd.isna(trans_currency) or pd.isna(customer_class) or pd.isna(product_sku):
            df.drop([row])
            continue

        if product_sku not in products:
            products.update({
                product_sku: products_template()
            })

        products[product_sku][trans_currency][customer_class]["data"].append(df.loc[row])
        
        print((len(df.index) - row)/(len(df.index)) * 100, "%")


    count = 0
    with concurrent.futures.ProcessPoolExecutor() as executor:

        for product in products.values():
            count += 1

            for currency in product.values():
                for market in currency.values():
                    if len(market["data"]) != 0:

                        executor.submit(populate_product_details(market))

            print(count/len(products) * 100, "%")
            if progress_callback:
                progress_callback(count/len(products))
        
    finished_callback([products, counts], output_path)
    return [products, counts]

def main():
    inputPath = input("Enter the path to the input file: ")
    outputPath = input("Enter the path to the output file: ")
    process_data(inputPath, outputPath, write_to_excel)

if __name__ == "__main__":
    main()