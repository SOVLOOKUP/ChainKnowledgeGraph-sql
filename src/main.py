import asyncio
# from db import Surrealdb
from json import loads

def load_json(s: str):
    try:
        return loads(s)
    except:
        if s.strip() != '': print(s)
        return None

domain="sovlookup.top"
port=8000
username="root"
password="root"
namespace = "test"
database = "chai"

async def main():
    with open("data.sql", "w") as f:
        # async with Surrealdb(f"http://{domain}:{port}",namespace,database,username,password) as client:
            async def add_sql(sql: str):
                try:
                    # print(await client.execute(sql))
                    f.write(sql + '\n')
                except:
                    print("非法输入", sql)

            # 产业实体
            with open(f"data/industry.json") as industrys:
                for industry in industrys.readlines():
                    industry_json = load_json(industry)
                    if industry_json == None: continue
                    await add_sql(f"CREATE industry:{industry_json['code']} SET name = '{industry_json['name']}';")

            # 公司实体
            with open(f"data/company.json") as companys:
                for company in companys.readlines():
                    company_json = load_json(company)
                    if company_json == None: continue
                    await add_sql(f"CREATE company:{company_json['code'].replace('.', '')} SET name = '{company_json['name']}', fullname = '{company_json['fullname']}', location = '{company_json['location']}', time = '{company_json['time']}';")

            # 产品实体
            with open(f"data/product.json") as products:
                for product in products.readlines():
                    product_json = load_json(product)
                    if product_json == None: continue
                    await add_sql(f"CREATE product SET name = '{product_json['name']}';")


            # 产业关系
            with open(f"data/industry_industry.json") as ups:
                for up in ups.readlines():
                    up_json = load_json(up)
                    if up_json == None: continue
                    await add_sql(f"RELATE industry:{up_json['from_code']}->industry_industry_relation->industry:{up_json['to_code']} SET name = '上游产业';")

            with open(f"data/industry_up.json") as ups:
                for up in ups.readlines():
                    up_json = load_json(up)
                    if up_json == None: continue
                    for k in up_json['ups'].keys():
                        await add_sql(f"RELATE (SELECT * FROM industry WHERE name = '{up_json['industry']}')->industry_industry_relation->(SELECT * FROM industry WHERE name = '{k}') SET name = '上游产业';")

            # 公司关系
            with open(f"data/company_industry.json") as companys:
                for company in companys.readlines():
                    company_json = load_json(company)
                    if company_json == None: continue
                    await add_sql(f"RELATE company:{company_json['company_code'].replace('.', '')}->company_industry_relation->industry:{company_json['industry_code']} SET name = '{company_json['rel']}';")

            with open(f"data/company_product.json") as companys:
                for company in companys.readlines():
                    company_json = load_json(company)
                    if company_json == None: continue
                    await add_sql(f"RELATE company:{company_json['company_code'].replace('.', '')}->company_product_relation->(SELECT * FROM product WHERE name = '{company_json['product_name']}') SET name = '{company_json['rel']}', weight = {company_json['rel_weight']};")

            # 产品关系
            with open(f"data/product_product.json") as products:
                for product in products.readlines():
                    product_json = load_json(product)
                    if product_json == None: continue
                    await add_sql(f"RELATE (SELECT * FROM product WHERE name = '{product_json['from_entity']}')->product_product_relation->(SELECT * FROM product WHERE name = '{product_json['to_entity']}') SET name = '{product_json['rel']}';")

asyncio.run(main())
