from lxml import html
import requests
import time
import re
import json
import pandas as pd


def parse(url):
    for _ in range(10):
        response = requests.get(url)
        if response.status_code == 200:
            success = True
            break
        else:
            print("Connection failed "%(response.status_code))
            success = False
    
    if success == False:
        print("Failed to process")
    
    parser = html.fromstring(response.text)
    listing = parser.xpath("//li[@class='regular-search-result']")
    raw_json = parser.xpath("//script[contains(@data-hypernova-key,'yelp_main__SearchApp')]//text()")
    scraped_datas = []
    
    if raw_json:
        cleaned_json = raw_json[0].replace('<!--', '').replace('-->', '').strip()
        json_loaded = json.loads(cleaned_json)
        search_results = json_loaded['searchPageProps']['searchResultsProps']['searchResults']
        
        for results in search_results:  
            result = results.get('searchResultBusiness')
            if result:
                is_ad = result.get('isAd')
                price_range = result.get('priceRange')
                position = result.get('ranking')
                name = result.get('name')
                ratings = result.get('rating')
                reviews = result.get('reviewCount')
                address = result.get('formattedAddress')
                neighborhood = result.get('neighborhoods')
                category_list = result.get('categories')
                full_address = address+' '+''.join(neighborhood)
                url = "https://www.yelp.com"+result.get('businessUrl')
                
                category = []
                for categories in category_list:
                    category.append(categories['title'])
                business_category = ','.join(category)

                #for not considering ads
                if not(is_ad):
                    data = {
                        'business_name': name,
                        'rank': position,
                        'review_count': reviews,
                        'categories': business_category,
                        'rating': ratings,
                        'address': full_address,
                        'price_range': price_range,
                        'url': url
                    }
                    scraped_datas.append(data)
        return scraped_datas


if __name__ == "__main__":    
  
    url1 = 'https://www.yelp.com/search?cflt='
    url2 = '&find_loc=New%20York%2C%20NY'
    business_type = ['bars','nightlife','restaurants','homeservices', 'auto', 'hair', 'dryclean', 'mobilephonerepair','gyms','massage','shopping' ]  ###types of business as categorized and displayed on main webpage
    
    for i in business_type:
        start = 1
        count = 0
        total_data = []
        while count<=990:            #####as for any section only pages till 100 or ~1000 entries are present and after that yelp website displays error
            if start ==1:
                url = url1 + i + url2             ####as first page of any section is without the start attribute
                start = 0 
            else:
                url = url1 + i + url2 + '&start=' + str(count)
                
            print ("Processing:", url)
            scraped_data = parse(url)
            total_data.append(pd.DataFrame(scraped_data))
            
            time.sleep(5)
            if i=='bars' or i=='nightlife':        #####as bars and nightlife pages show 30 results at a time
                count+=30
            else:
                count+=10
            if (i=='bars' and count>960)or(i=='nightlife' and count>960):                 ###last page available for bars and nightlife are 960 instead of 990 as in other cases(because shiws thirty results simultaneously)
                break

            
        new_df = pd.concat(total_data)
        new_df.to_csv('yelpfinaldataset.csv',mode='a',index=False, encoding='utf-8')

    
  
