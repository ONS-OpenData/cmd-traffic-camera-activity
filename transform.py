from databaker.framework import *
import pandas as pd
import datetime, glob

## these needed to be pointed to if not pip installed 
from databakerUtils.sparsityFunctions import SparsityFiller 
from databakerUtils.v4Functions import v4Integers 
from databakerUtils.writers import v4Writer 
from api_pipeline import Multi_Upload_To_Cmd 

### file paths that may need to be changed ###
location = 'inputs/traffic-camera-activity/*.xlsx' # path to source data
output = 'D:/' # output file written here
metadata_file = 'inputs/traffic-camera-activity/traffic-camera-activity-time-series-v4.csv-metadata.json' # path to metadata file
credentials = 'florence-details.json' # path to login details

def transform(location, output):
    file = glob.glob(location)
    assert len(file) == 1, f"should onlybe one input file not '{len(file)}'"
    file = file[0]
    output_file = output + 'v4-traffic-camera-activity.csv'

    tabs = loadxlstabs(file)
    tabs = [tab for tab in tabs if 'note' not in tab.name.lower()] # remove note tab

    conversionsegments = []
    for tab in tabs:
        junk = tab.filter(contains_string('Note:')).expand(DOWN).expand(RIGHT) # unwated notes

        date = tab.excel_ref('A').expand(DOWN).is_not_blank().is_not_whitespace()
        date -= junk

        area = tab.excel_ref('B1').expand(RIGHT).is_not_blank().is_not_whitespace()

        vehicle = tab.excel_ref('B2').expand(RIGHT).is_not_blank().is_not_whitespace()

        adjustment = tab.name

        obs = area.waffle(date)

        dimensions = [
                HDimConst(TIME, 'year'), # year to be filled in later
                HDimConst(GEOG, 'K02000001'),
                HDim(date, 'date', DIRECTLY, LEFT),
                HDim(area, 'area', DIRECTLY, ABOVE),
                HDim(vehicle, 'vehicle', DIRECTLY, ABOVE),
                HDimConst('adjustment', adjustment)
                ]

        for cell in dimensions[2].hbagset:
            dimensions[2].AddCellValueOverride(cell, str(cell.value))

        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)

    data = pd.concat(conversionsegments)
    df = v4Writer('file-path', data, asFrame=True) 

    '''Post Processing'''
    df['V4_1'] = df['V4_1'].apply(v4Integers)

    df['Time_codelist'] = df['date'].apply(DateToYear)
    df['Time'] = df['Time_codelist']

    df['Geography'] = 'United Kingdom'

    df['date_codelist'] = df['date'].apply(DateToDDMMM)
    df['date'] = df['date_codelist']
    df['date_codelist'] = df['date_codelist'].apply(lambda x: x.lower())

    df['area_codelist'] = df['area'].apply(Slugize)

    df['vehicle'] = df['vehicle'].apply(lambda x: x.capitalize())
    df['vehicle_codelist'] = df['vehicle'].apply(Slugize)

    df['adjustment'] = df['adjustment'].apply(SeasonalAdjustment)
    df['adjustment_codelist'] = df['adjustment'].apply(Slugize)

    df = df.rename(columns={
            'Time_codelist':'calendar-years',
            'Geography_codelist':'uk-only',
            'date_codelist':'dd-mm',
            'date':'DayMonth',
            'area_codelist':'traffic-camera-area',
            'area':'TrafficCameraArea',
            'vehicle_codelist':'pedestrians-and-vehicles',
            'vehicle':'PedestriansAndVehicles',
            'adjustment_codelist':'seasonal-adjustment',
            'adjustment':'SeasonalAdjustment'
            }
        )

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, '*')

def DateToYear(value):
    # pulls the year from the date -> date should have form 'yyyy-mm-dd 00:00:00'
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.datetime.strftime(as_datetime, '%Y')
    return new_value

def DateToDDMMM(value):
    # converts date to just day and month -> dd-mmm
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.datetime.strftime(as_datetime, '%d-%m')
    return new_value

def Slugize(value):
    new_value = value.replace('&', 'and').replace(' ', '-').lower()
    return new_value

def SeasonalAdjustment(value):
    if value == 'Trend':
        return value
    lookup = {
            'Non seasonally adjusted':'Non Seasonal Adjustment',
            'Seasonally adjusted':'Seasonal Adjustment'
            }
    return lookup[value]

# Run transform
if __name__ == '__main__':
    # variables for upload
    dataset_id = 'traffic-camera-activity'
    edition = 'time-series'
    collection_name = 'CMD traffic camera activity'
    print(f"Uploading {dataset_id} to CMD")

    upload_dict = {
            dataset_id:{
                    'v4':output_file,
                    'edition':edition,
                    'collection_name':collection_name,
                    'metadata_file':metadata_file
                    }
            }

    Multi_Upload_To_Cmd(credentials, upload_dict)