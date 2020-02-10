import requests
import json

# reference link: 
# http://cuihuan.net/wuhan/nCoV.html

def get_json_file(output_file): 
    '''
    output_file: the file name will be saved. 
    this is to get and save the json file for later usage
    '''
    response = requests.get('https://lab.isaaclin.cn/nCoV/api/area?latest=0')

    response.encoding = 'utf-8'

    json_data = json.loads(response.text)

    with open(output_file, 'w') as write_file: 
        json.dump(json_data, write_file)
class Deal_nCoV_data(object): 
    def __init__(self, input_name): 
        self.input_name = input_name

    def prepare_df(self): 
        with open(self.input_name, 'r') as read_file: 
            raw_data = json.load(read_file)

        # print (raw_data['results'][0]['provinceName'], raw_data['results'][0]['country'], raw_data['results'][0]['cities'], raw_data['results'][0]['updateTime'])
        raw_data_ls = raw_data['results']
        country_dict = dict()
        for i in raw_data_ls: 
            temp_country = i['country']
            temp_provice = i['provinceName']
            if temp_country not in country_dict.keys(): 
                country_dict[temp_country] = [temp_provice]
            elif temp_provice in country_dict[temp_country]: continue
            else: 
                country_dict[temp_country].append(temp_provice)

        for k, v in country_dict.items(): 
            if len(v) == 1: continue
            else: 
                print (k, len(v))
        # print (raw_data['results'][2]['provinceName'], raw_data['results'][0]['country'], raw_data['results'][0]['cities'], raw_data['results'][0]['updateTime'])
        # print (len(raw_data['results']))
        # res_dict = todos['results']
        # print (len(res_dict[0]))

        # suc_dict = todos['success']
        # print (suc_dict)


        # print (res_dict['confirmedCount'])

    def final_run(self): 
        self.prepare_df()

if __name__ == '__main__': 
    raw_data_fl = 'nCoV_data.json'

    # # get data
    # get_json_file(raw_data_fl)

    # ana data
    nCov_stat = Deal_nCoV_data(raw_data_fl)
    nCov_stat.final_run()

    