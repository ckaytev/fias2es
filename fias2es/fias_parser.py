import datetime
import glob
import os
import time
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

hadobjd = pd.DataFrame()

# These are the utilities I'm gonna use, it's just nice
# to have them all gathered in one place. I provided some short descriptions,
# and I might or might not explain what's going on a bit later.
def parse_xml(x):
    """
    Parse GAR XML file into pandas dataframe object
    """
    tree = ET.parse(x)
    root = tree.getroot()
    df = [child.attrib for child in root]
    df = pd.DataFrame.from_dict(df)
    return df


def get_town(x):
    """
    Chain post-cleanup.
    """
    priority = ['5', '6', '4', '7', '1']
    street = [f'{i}' for i in range(8, 0, -1)]
    streets = [p for p in street if x[p] == 1]
    if len(streets) == 0:
        street = None
    else:
        street = streets[0]
    town = [p for p in priority if p != street and x[p] == 1]
    town = town[0] if len(town) > 0 else None
    leftover = [
        x for x in streets
        if x != street
        and x != town
        and x not in ['1', '2', '3']
    ]
    muni = [x for x in streets if x in ['2', '3']]
    region = '1'
    return street, town, leftover, muni, region


def get_struct_addr(x):
    global hadobjd
    region = hadobjd[x[x['region']]]
    region = ' '.join([region['TYPELONGNAME'].lower(), region['NAME']])

    if x['town'] != 'nan' and x[x['town']] in hadobjd:
        town = hadobjd[x[x['town']]]
        town = ' '.join([town['TYPELONGNAME'].lower(), town['NAME']])
    else:
        town = ''

    if x['street'] != 'nan' and x[x['street']] in hadobjd:
        street = hadobjd[x[x['street']]]
        street = ' '.join([street['TYPELONGNAME'].lower(), street['NAME']])
    else:
        street = ''

    house = hadobjd[x['10']]

    extra_house = []
    if house['HOUSENUM1'] == house['HOUSENUM1']:
        extra_house.extend([house['TYPELONGNAME1'].lower(), house['HOUSENUM1']])
    if house['HOUSENUM2'] == house['HOUSENUM2']:
        extra_house.extend([house['TYPELONGNAME2'].lower(), house['HOUSENUM2']])
    extra_house = ' '.join(extra_house)

    house = ' '.join([house['TYPELONGNAME'].lower(), house['HOUSENUM']])

    leftover = []
    for y in x['leftover']:
        leftover.append(hadobjd[x[y]]['TYPELONGNAME'])
        leftover.append(hadobjd[x[y]]['NAME'])
    leftover = ' '.join(leftover)

    muni = []
    for y in x['muni']:
        muni.append(hadobjd[x[y]]['TYPELONGNAME'])
        muni.append(hadobjd[x[y]]['NAME'])
    muni = ' '.join(muni)
    return {
        'id': x['10'],
        'region': region,
        'town': town,
        'street': street,
        'house': house,
        'extra_house': extra_house,
        'leftover': leftover,
        'muni': muni
    }

def parser(region_id):
    # We'll be working on one particular region
    region = os.path.join('data', str(region_id))

    # Read Address Object File
    # XML files in the DB are quite similar in structure, 
    # so we'll use parse_xml utility to convert XML files to pandas DataFrames.
    # Filter out historical data by ISACTUAL and ISACTIVE attributes.
    fname = glob.glob(os.path.join(region, 'AS_ADDR_OBJ_*.XML'))
    fname = [x for x in fname if 'PARAMS' not in x and 'DIVISION' not in x]
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    adobj = parse_xml(fname)
    adobj = adobj[(adobj['ISACTUAL'] == '1') & (adobj['ISACTIVE'] == '1')]

    # Read Address Object Types File
    # The way of joining address object types is a bit messed up, 
    # just like a lot of other stuff in here. We'll rename some columns 
    # right away as to not to string along same info in different columns.
    fname = glob.glob('data/AS_ADDR_OBJ_TYPES_*.XML')
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    adobjt = parse_xml(fname)

    adobj = adobj.merge(
        adobjt[['SHORTNAME', 'DESC', 'LEVEL']].rename(
            columns={
                'SHORTNAME': 'TYPENAME',
                'DESC': 'TYPELONGNAME'
            }
        ),
        on=['LEVEL', 'TYPENAME']
    )

    # Read Address Object Levels File
    # Levels are not as strict as one might think and 
    # similar address objects ofthen end up on different levels.
    fname = glob.glob('data/AS_OBJECT_LEVELS_*.XML')
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    lev = parse_xml(fname)

    adobj = adobj.merge(
        lev[['NAME', 'LEVEL']].rename(
            columns={
                'NAME': 'LEVELNAME'
            }
        ),
        on='LEVEL'
    )

    # Read Houses File
    # Reading houses data process is more or less the same.
    fname = glob.glob(os.path.join(region, 'AS_HOUSES_*.XML'))
    fname = [x for x in fname if 'PARAMS' not in x]
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    hous = parse_xml(fname)
    hous = hous.rename(
        columns={
            'ADDTYPE1': 'HOUSETYPE1',
            'ADDTYPE2': 'HOUSETYPE2',
            'ADDNUM1': 'HOUSENUM1',
            'ADDNUM2': 'HOUSENUM2'
        }
    )
    if 'ISACTUAL' in hous.columns:
        hous = hous[(hous['ISACTUAL'] == '1') & (hous['ISACTIVE'] == '1')]
    else:
        hous = hous[(hous['ISACTIVE'] == '1')]

    fname = glob.glob('data/AS_HOUSE_TYPES_*.XML')
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    houst = parse_xml(fname)
    houst = houst.rename(
        columns={
            'SHORTNAME': 'TYPENAME',
            'DESC': 'TYPELONGNAME',
            'ID': 'HOUSETYPE'
        }
    )

    # Read Houses Types File
    # Here's the main difference: two additional columns to store buildings.
    fname = glob.glob('data/AS_ADDHOUSE_TYPES_*.XML')
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    housta = parse_xml(fname)
    housta = housta.rename(
        columns={
            'SHORTNAME': 'TYPENAME',
            'DESC': 'TYPELONGNAME',
            'ID': 'HOUSETYPE'
        }
    )

    hous = hous.merge(
        houst[[
            'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
        ]].drop_duplicates(),
        on='HOUSETYPE'
    )
    if 'HOUSETYPE1' in hous.columns:
        hous = hous.merge(
            housta[[
                'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
            ]].rename(
                columns={
                    'HOUSETYPE': 'HOUSETYPE1'
                }
            ).drop_duplicates(),
            on='HOUSETYPE1',
            how='left',
            suffixes=(None, '1')
        )
    else:
        hous['HOUSETYPE1'] = np.nan
        hous['TYPELONGNAME1'] = np.nan
        hous['HOUSENUM1'] = np.nan
        hous['TYPENAME1'] = np.nan
    if 'HOUSETYPE2' in hous.columns:
        hous = hous.merge(
            housta[[
                'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
            ]].rename(
                columns={
                    'HOUSETYPE': 'HOUSETYPE2'
                }
            ).drop_duplicates(),
            on='HOUSETYPE2',
            how='left',
            suffixes=(None, '2')
        )
    else:
        hous['HOUSETYPE2'] = np.nan
        hous['TYPELONGNAME2'] = np.nan
        hous['HOUSENUM2'] = np.nan
        hous['TYPENAME2'] = np.nan

    hous['LEVEL'] = '10'
    hous['LEVELNAME'] = 'Здание/Сооружение'
    hous['NAME'] = hous[['TYPELONGNAME', 'HOUSENUM']].apply(
        lambda x: (
            x['TYPELONGNAME'].lower() + ' '
            if x['TYPELONGNAME'] == x['TYPELONGNAME']
            else ''
        ) + x['HOUSENUM'],
        axis=1
    )
    hous['NAME1'] = hous[['TYPELONGNAME1', 'HOUSENUM1']].apply(
        lambda x: (
            x['TYPELONGNAME1'].lower() + ' '
            if x['TYPELONGNAME1'] == x['TYPELONGNAME1']
            else ''
        ) + (
            x['HOUSENUM1']
            if x['HOUSENUM1'] == x['HOUSENUM1']
            else ''
        ),
        axis=1
    )
    hous['NAME2'] = hous[['TYPELONGNAME2', 'HOUSENUM2']].apply(
        lambda x: (
            x['TYPELONGNAME2'].lower() + ' '
            if x['TYPELONGNAME2'] == x['TYPELONGNAME2']
            else ''
        ) + (
            x['HOUSENUM2']
            if x['HOUSENUM2'] == x['HOUSENUM2']
            else ''
        ),
        axis=1
    )

    # Let's concat our address objects with house objects.
    hadobj = pd.concat(
        [
            adobj[[
                'OBJECTID', 'OBJECTGUID', 'NAME', 'TYPENAME', 'LEVEL',
                'ISACTUAL', 'ISACTIVE', 'TYPELONGNAME', 'LEVELNAME'
            ]],
            hous[[
                'OBJECTID', 'OBJECTGUID', 'HOUSENUM', 'HOUSETYPE',
                'TYPENAME', 'TYPELONGNAME', 'HOUSENUM1', 'HOUSETYPE1',
                'TYPENAME1', 'TYPELONGNAME1', 'HOUSENUM2', 'HOUSETYPE2',
                'TYPENAME2', 'TYPELONGNAME2', 'ISACTUAL', 'ISACTIVE',
                'LEVEL', 'NAME', 'NAME1', 'NAME2', 'LEVELNAME'
            ]]
        ],
        sort=True,
        ignore_index=True
    )

    # Read Administrative Relations File
    # More exciting part is coming, I guess. The important thing is whether
    # PATH column is included or not. Cause if it isn't, we suffer in attempts
    # to restore paths ourselves.
    fname = glob.glob(os.path.join(region, 'AS_MUN_HIERARCHY_*.XML'))
    if len(fname) != 1:
        msg = f'Please check file count for region {region} there are {len(fname)} files'
        logger.error(msg)
        raise Exception(msg)
    fname = fname[0]
    adm = parse_xml(fname)
    adm = adm[
        adm.ENDDATE.apply(
            lambda x: datetime.datetime.strptime(
                x, '%Y-%m-%d'
            ) > datetime.datetime.fromtimestamp(
                time.time()
            )
        )
    ]
    cols = ['OBJECTID', 'PARENTOBJID', 'PATH']
    adm0 = adm[adm['ISACTIVE'] == '1'][cols].merge(
        hadobj[(hadobj['ISACTUAL'] == '1') & (hadobj['ISACTIVE'] == '1')],
        on='OBJECTID'
    )

    # Building Address Chains
    # split them up nicely starting from house objects (level 10).
    chains = [
        tuple(y for y in reversed(x.split('.')))
        for x in tqdm(adm0[adm0['LEVEL'] == '10']['PATH'])
    ]
    global hadobjd
    hadobjd = hadobj.set_index('OBJECTID').to_dict('index')

    # Time to process chains in a fashionable manner.
    dfch = pd.DataFrame()
    dfch['chain'] = list(set(chains))

    dfch['levchain'] = [
        tuple([hadobjd[y]['LEVEL'] for y in x if y != '0' and y in hadobjd])
        for x in tqdm(dfch['chain'])
    ]
    dat = [
        {
            m: l
            for m, l in zip(x, y)
        }
        for x, y in zip(dfch['levchain'], dfch['chain'])
    ]
    for i in range(10, 0, -1):
        dfch[f'{i}'] = [
            d[f'{i}']
            if f'{i}' in d
            else None
            for d in dat
        ]

    # Now we'll attempt to structure all those chains, 
    # so that they fit into old-fashioned way of saying the address.
    chl = list(set(dfch['levchain'].apply(lambda x: '-'.join(x))))
    df = pd.DataFrame()
    df['levchain'] = chl
    for i in range(10, 0, -1):
        dat = [(f'{i}' in y.split('-')) * 1 for y in chl]
        df[f'{i}'] = dat

    lst = df.apply(get_town, axis=1)
    df['street'] = [x[0] for x in lst]
    df['town'] = [x[1] for x in lst]
    df['leftover'] = [x[2] for x in lst]
    df['muni'] = [x[3] for x in lst]
    df['region'] = [x[4] for x in lst]
    df['levchain'] = df['levchain'].apply(lambda x: tuple(x.split('-')))

    # Now join chains and their structure together.
    dfch = dfch.merge(
        df[['levchain', 'street', 'town', 'leftover', 'muni', 'region']],
        on='levchain',
    )

    # Structured Addresses
    # Let's give our addresses more familiar structure and 
    # look at some confusing examples.
    struct_addresses = dfch.apply(get_struct_addr, axis=1)
    return pd.DataFrame.from_records(struct_addresses)
