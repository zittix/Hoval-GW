import openpyxl
import json
import os

wb = openpyxl.load_workbook(filename = 'TTE-GW-Modbus-datapoints.xlsx', read_only=True)



if os.path.exists('db.json'):
    with open('db.json', 'r') as f:
        try:
            ret = json.load(f)
        except:
            ret = {}
else:
    ret = {}

def parse_and_merge(ws, lang, ret):
    header_found = False
    for row in ws.rows:
        if not header_found:
            header_found = True
            continue
        i = 0
        dpid=[]
        descr=None
        point = {
            "texts": {}
        }
        for cell in row:
            if i >= 3 and i <=5:
                dpid.append(str(cell.value))
            elif i == 1:
                dpid.append(cell.value)
                point['device'] = cell.value
            elif i == 6:
                point['descr'] = {lang: cell.value}
            elif i == 8:
                point['type'] = cell.value
            elif i == 9:
                point['decimal'] = cell.value
            elif i == 10:
                point['function_group'] = {lang: cell.value}
            elif i == 11:
                point['function_name'] = {lang: cell.value}
            elif i == 12:
                point['steps'] = cell.value
            elif i == 13:
                point['min'] = cell.value
            elif i == 14:
                point['max'] = cell.value
            elif i == 15:
                point['writable'] = True if cell.value and cell.value.lower() == 'Yes' else False
            elif i == 16:
                point['unit'] = cell.value
            elif i == 17:
                point['comment'] = {lang: cell.value}
            elif i > 17 and cell.value:
                point['texts'][i - 18] = {lang: cell.value }
            i+=1
        dpid = "-".join(dpid)
        if dpid not in ret:
            ret[dpid] = point
        else:
            ret[dpid]['descr'][lang] = point['descr'][lang]
            ret[dpid]['function_group'][lang] = point['function_group'][lang]
            ret[dpid]['function_name'][lang] = point['function_name'][lang]
            ret[dpid]['comment'][lang] = point['comment'][lang]
            for i in ret[dpid]['texts'].keys():
                if i in point['texts']:
                    ret[dpid]['texts'][i][lang] = point['texts'][i][lang]

parse_and_merge( wb.worksheets[0], 'de', ret)
parse_and_merge( wb.worksheets[1], 'en', ret)
parse_and_merge( wb.worksheets[2], 'fr', ret)
parse_and_merge( wb.worksheets[3], 'it', ret)

with open('db.json', 'w') as f:
    json.dump(ret, f)