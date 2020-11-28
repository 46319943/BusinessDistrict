import pandas as pd
import json
from slab.geocode.geocode_async import geocode_dataframe_async
import asyncio


def main():
    with open('business_district.json', encoding='UTF-8') as f:
        bd_json = json.load(f)

    result_list = list()

    for province_object in bd_json:
        province_name = province_object['name']
        province_city_list = province_object['cities']

        for city_object in province_city_list:
            city_name = city_object['name']
            city_county_list = city_object['counties']

            for county_object in city_county_list:
                county_name = county_object['name']
                county_circle_list = county_object['circles']

                for circle_object in county_circle_list:
                    circle_name = circle_object['name']

                    if '其他' in circle_name:
                        continue

                    result_list.append({
                        'province': province_name,
                        'city': city_name,
                        'county': county_name,
                        'circle': circle_name
                    })

    df = pd.DataFrame(result_list)
    df.to_csv('business_district.csv', index=False, encoding='UTF-8')

    df = asyncio.run(geocode_dataframe_async(
        df, '', ['province', 'city', 'county', 'circle']))
    df.to_csv('geocode_result.csv', index=False, encoding='UTF-8')


main()
