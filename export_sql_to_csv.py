from sqlalchemy import create_engine, text
import config
from tqdm import tqdm

import pandas as pd

engine = create_engine(config.DB.ethereum(), echo=False)

if __name__ == '__main__' :

    n = 1000000
    row = 11409004

    num = int(row/n)

    count_f = n
    count = n

    c = count_f

    sql_query = []

    for i in tqdm(range(num+1)):

        if(count_f == count  ):
            sql_query.append("SELECT * FROM ethereum.acc_trans_price LIMIT " + str(count))
            count += count_f

        else:
            sql_query.append("SELECT * FROM ethereum.acc_trans_price LIMIT " + str(c) + " ," + str(n))
            c = count
            count += count_f

    count = 0
    for i in tqdm(sql_query):
        name = "000"+ str(count)
        df = pd.read_sql(i, engine)
        df.to_csv(f'data/acc_trans_price{name}.csv')
        count += 1
