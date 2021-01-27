import requests
import pandas as pd



def extract_data(base_url, suffix, 
                 query, param_url, 
                 num_pages):
    
    results_api = []
    offset = 0
    while offset <= num_pages:
        print(offset)
        try:
            first_response = requests.get(base_url 
                                          + suffix 
                                          + query 
                                          + param_url 
                                          + str(offset))
            response_list=first_response.json()
            results_api.append(response_list['results'])
        except:
            pass
        offset += 1
        
            
    return results_api


def generate_dataframe(api_data, query):
    
    data = []
    for result in api_data:
        for response in result:  
            try:
                data.append({
                    "title": response['title'], 
                    "price": response['price'],
                    "domain_id": response['domain_id'],
                    "condition": response['condition'],
                    "accepts_mercadopago": response['accepts_mercadopago'],
                    "free_shipping": response['shipping']['free_shipping'],
                    "country_seller": response['seller_address']['country']["name"],
                    "original_price": response['original_price'],
                    "available_quantity": response['available_quantity'],
                    "total_transactions_seller": response['seller']['seller_reputation']['transactions']['total'],
                    "canceled_transactions_seller": response['seller']['seller_reputation']['transactions']['canceled'],
                    "completed_transactions_reseller": response['seller']['seller_reputation']['transactions']['completed'],
                    "positive_rating": response['seller']['seller_reputation']['transactions']['ratings']['positive'],
                    "power_seller_status": response['seller']['seller_reputation']['power_seller_status'],
                    "negative_rating": response['seller']['seller_reputation']['transactions']['ratings']['negative'],
                    "neutral_rating": response['seller']['seller_reputation']['transactions']['ratings']['neutral'],
                    "delayed_handlingtime_rate": response['seller']['seller_reputation']['metrics']['delayed_handling_time']['rate'],
                    "delayed_handlingtime_value": response['seller']['seller_reputation']['metrics']['delayed_handling_time']['value'],
                    "seller_sales_completed": response['seller']['seller_reputation']['metrics']['sales']['completed'],
                    "seller_cancellations_rate": response['seller']['seller_reputation']['metrics']['cancellations']['rate'],
                    "seller_cancellations_value": response['seller']['seller_reputation']['metrics']['cancellations']['value'],
                    "currency_id": response['currency_id'],
                    "sold_quantity": response['sold_quantity'],  
                    "category": query
                })
            except:
                pass
    
    dataset = pd.DataFrame(data)
    return dataset




if __name__ == "__main__":
    
    base_url = "https://api.mercadolibre.com" 
    suffix =  "/sites/MLA/search?q="
    param_url = "&offset="
    num_pages = 1000
    
    results_api_celular = extract_data(base_url, suffix,
                                      "celular", param_url, num_pages )
    
    results_api_tv = extract_data(base_url, suffix,
                                      "tv", param_url, num_pages )
    
    
    results_api_consoles = extract_data(base_url, suffix,
                                      "consola", param_url, num_pages )
    
    
    results_api_computer = extract_data(base_url, suffix,
                                      "computador", param_url, num_pages )
    
    
    
    df_celulares = generate_dataframe(results_api_celular, 
                                      "celulares")
    
    df_tv = generate_dataframe(results_api_tv, 
                               "tv")
    
    df_consoles = generate_dataframe(results_api_consoles, 
                                     "consola")
    
    df_computers = generate_dataframe(results_api_computer, 
                                      "computador")
    
    full_dataset = pd.concat([df_tv, df_celulares,
                              df_consoles, df_computers])
    
    
    full_dataset.to_csv('data/export_dataframe.csv', 
                        index = False, 
                        header=True)
    
    print("dataset creado")
    
    
   