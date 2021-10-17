import multiprocessing as mp
import numpy as np
import pandas as pd
import time
import concurrent.futures
import time


np.random.seed(321)

global df, temp_df

# ======================== Generate Sample Data =============================
# store names
stores = ["store_a", "store_b", "store_c", "store_d", "store_e"]

# number of daily sales
number_of_sales_per_store = 5
df = pd.DataFrame(columns=["store_name", "sales"], dtype=np.int8)
for store in stores:
    d = pd.DataFrame(
        dict(
            store_name=[store for _ in range(number_of_sales_per_store)],
            sales=[
                np.random.randint(100, 1000) for _ in range(number_of_sales_per_store)
            ],
        )
    )
    df = df.append(d)

# create a place holder data frame and save it
results = pd.DataFrame(columns=["store_name", "average_sales"])
results.to_csv("results.csv", index=False)


# create a shared dictionary
# if you are using windows, create this dictionary
# inside the if __name__ == condition, just above the
# with statement
global shared_dictionary
shared_dictionary = mp.Manager().dict()


# target function
def target_function(store):

    df_slice = df[df["store_name"] == store]
    mean_of_slice = np.mean(df_slice["sales"])
    # method 1: save to shared dictionary
    shared_dictionary[store] = [mean_of_slice]
    # print(shared_dictionary)
    # method 2: save to the place holder csv file
    # first save results to pandas df, then append the results to
    # the csv place holder we created already
    temp_df = pd.DataFrame(columns=["store_name", "average_sales"])
    temp_df["store_name"] = [store]
    temp_df["average_sales"] = [mean_of_slice]
    temp_df.to_csv("results.csv", mode="a", index=False, header=False)
    # temp_df.to_csv(f"{store}_results.csv", mode="a", index=False, header=False)
    # return {"store_name": store, "average_sales": mean_of_slice}


# run the transformation in parallel
if __name__ == "__main__":

    s = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        result = executor.map(
            target_function,
            stores,
        )

    e = time.perf_counter()
    print(shared_dictionary)
    print(f"parallel took {e-s} sec to run ")
