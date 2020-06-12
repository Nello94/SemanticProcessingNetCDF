import json

from NetCDF2JSON import NetCDF2JSON

if __name__ == "__main__":
    filename = "data/ww33_d01_20200329Z1200.nc4"
    
    
    #filename = "http://data.meteo.uniparthenope.it/opendap/opendap/rms3" \
    #           "/d03/history/2020/03/29/rms3_d03_20200329Z1200.nc"

    netCDF2JSON=NetCDF2JSON(filename)

    item = netCDF2JSON.as_json()
    #print(json.dumps(item))

    with open("data/ww33_d01_20200329Z1200/json.json", 'w') as f:
        f.write("[\n\t")
        f.write(json.dumps(item, ensure_ascii=False, indent=4))
        f.write("\n]")
    f.close()
